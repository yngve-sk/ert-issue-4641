import functools
import re
from contextlib import ExitStack as does_not_raise
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr
import xtgeo
from iterative_ensemble_smoother import steplength_exponential
from tabulate import tabulate

from ert.analysis import (
    ErtAnalysisError,
    ObservationStatus,
    iterative_smoother_update,
    smoother_update,
)
from ert.analysis._es_update import (
    _load_observations_and_responses,
    _load_param_ensemble_array,
    _save_param_ensemble_array_to_disk,
)
from ert.analysis.event import AnalysisCompleteEvent, AnalysisErrorEvent
from ert.config import Field, GenDataConfig, GenKwConfig
from ert.config.analysis_config import UpdateSettings
from ert.config.analysis_module import ESSettings, IESSettings
from ert.config.gen_kw_config import TransformFunctionDefinition
from ert.field_utils import Shape


@pytest.fixture
def uniform_parameter():
    return GenKwConfig(
        name="PARAMETER",
        forward_init=False,
        template_file="",
        transform_function_definitions=[
            TransformFunctionDefinition("KEY1", "UNIFORM", [0, 1]),
        ],
        output_file="kw.txt",
        update=True,
    )


@pytest.fixture
def obs():
    return xr.Dataset(
        {
            "observations": (["report_step", "index"], [[1.0, 1.0, 1.0]]),
            "std": (["report_step", "index"], [[0.1, 1.0, 10.0]]),
        },
        coords={"index": [0, 1, 2], "report_step": [0]},
        attrs={"response": "RESPONSE"},
    )


def remove_timestamp_from_logfile(log_file: Path):
    with open(log_file, "r", encoding="utf-8") as fin:
        buf = fin.read()
    buf = re.sub(
        r"Time: [0-9]{4}\.[0-9]{2}\.[0-9]{2} [0-9]{2}\:[0-9]{2}\:[0-9]{2}", "Time:", buf
    )
    with open(log_file, "w", encoding="utf-8") as fout:
        fout.write(buf)


@pytest.mark.integration_test
@pytest.mark.flaky(reruns=5)
@pytest.mark.parametrize(
    "misfit_preprocess", [[["*"]], [], [["FOPR"]], [["FOPR"], ["WOPR_OP1_1*"]]]
)
def test_update_report(
    snake_oil_case_storage,
    snake_oil_storage,
    misfit_preprocess,
    snapshot,
):
    """
    Note that this is now a snapshot test, so there is no guarantee that the
    snapshots are correct, they are just documenting the current behavior.
    """
    ert_config = snake_oil_case_storage
    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")
    posterior_ens = snake_oil_storage.create_ensemble(
        prior_ens.experiment_id,
        ensemble_size=ert_config.model_config.num_realizations,
        iteration=1,
        name="new_ensemble",
        prior_ensemble=prior_ens,
    )
    events = []

    smoother_update(
        prior_ens,
        posterior_ens,
        list(ert_config.observations.keys()),
        ert_config.ensemble_config.parameters,
        UpdateSettings(auto_scale_observations=misfit_preprocess),
        ESSettings(inversion="subspace"),
        progress_callback=events.append,
    )

    event = next(e for e in events if isinstance(e, AnalysisCompleteEvent))
    snapshot.assert_match(
        tabulate(event.data.data, floatfmt=".3f") + "\n", "update_log"
    )


def test_update_report_with_exception_in_analysis_ES(
    snapshot,
    snake_oil_case_storage,
    snake_oil_storage,
):
    ert_config = snake_oil_case_storage
    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")
    posterior_ens = snake_oil_storage.create_ensemble(
        prior_ens.experiment_id,
        ensemble_size=ert_config.model_config.num_realizations,
        iteration=1,
        name="new_ensemble",
        prior_ensemble=prior_ens,
    )
    events = []

    with pytest.raises(
        ErtAnalysisError, match="No active observations for update step"
    ):
        smoother_update(
            prior_ens,
            posterior_ens,
            list(ert_config.observations.keys()),
            ert_config.ensemble_config.parameters,
            UpdateSettings(alpha=0.0000000001),
            ESSettings(inversion="subspace"),
            progress_callback=events.append,
        )

    error_event = next(e for e in events if isinstance(e, AnalysisErrorEvent))
    assert error_event.error_msg == "No active observations for update step"
    snapshot.assert_match(
        tabulate(error_event.data.data, floatfmt=".3f") + "\n", "error_event"
    )


@pytest.mark.parametrize(
    "update_settings",
    [
        UpdateSettings(alpha=0.1),
        UpdateSettings(std_cutoff=0.1),
        UpdateSettings(alpha=0.1, std_cutoff=0.1),
    ],
)
def test_update_report_with_different_observation_status_from_smoother_update(
    update_settings,
    snake_oil_case_storage,
    snake_oil_storage,
):
    ert_config = snake_oil_case_storage
    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")

    posterior_ens = snake_oil_storage.create_ensemble(
        prior_ens.experiment_id,
        ensemble_size=ert_config.model_config.num_realizations,
        iteration=1,
        name="new_ensemble",
        prior_ensemble=prior_ens,
    )
    events = []

    ss = smoother_update(
        prior_ens,
        posterior_ens,
        list(ert_config.observations.keys()),
        ert_config.ensemble_config.parameters,
        update_settings,
        ESSettings(inversion="subspace"),
        progress_callback=events.append,
    )

    outliers = len(
        [e for e in ss.update_step_snapshots if e.status == ObservationStatus.OUTLIER]
    )
    std_cutoff = len(
        [
            e
            for e in ss.update_step_snapshots
            if e.status == ObservationStatus.STD_CUTOFF
        ]
    )
    missing = len(
        [
            e
            for e in ss.update_step_snapshots
            if e.status == ObservationStatus.MISSING_RESPONSE
        ]
    )
    active = len(ss.update_step_snapshots) - outliers - std_cutoff - missing

    update_event = next(e for e in events if isinstance(e, AnalysisCompleteEvent))
    data_section = update_event.data
    assert data_section.extra["Active observations"] == str(active)
    assert data_section.extra["Deactivated observations - missing respons(es)"] == str(
        missing
    )
    assert data_section.extra[
        "Deactivated observations - ensemble_std > STD_CUTOFF"
    ] == str(std_cutoff)
    assert data_section.extra["Deactivated observations - outliers"] == str(outliers)


@pytest.mark.parametrize(
    "module, expected_gen_kw",
    [
        (
            "IES_ENKF",
            [
                0.5167529669896218,
                -0.9178847938402281,
                -0.6299046429604261,
                -0.1632005925319205,
                0.0216488942750398,
                0.07464619425897459,
                -1.5587692532545538,
                0.22910522740018124,
                -0.7171489000139469,
                0.7287252249699406,
            ],
        ),
        (
            "STD_ENKF",
            [
                1.3040645145742686,
                -0.8162878122658299,
                -1.5484856041224397,
                -1.379896334985399,
                -0.510970027650022,
                0.5638868158813687,
                -2.7669280724377487,
                1.7160680670028017,
                -1.2603717378211836,
                1.2014197463741136,
            ],
        ),
    ],
)
def test_update_snapshot(
    snake_oil_case_storage,
    snake_oil_storage,
    module,
    expected_gen_kw,
):
    """
    Note that this is now a snapshot test, so there is no guarantee that the
    snapshots are correct, they are just documenting the current behavior.
    """
    ert_config = snake_oil_case_storage

    # Making sure that row scaling with a row scaling factor of 1.0
    # results in the same update as with ES.
    # Note: seed must be the same!
    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")
    posterior_ens = snake_oil_storage.create_ensemble(
        prior_ens.experiment_id,
        ensemble_size=ert_config.model_config.num_realizations,
        iteration=1,
        name="posterior",
        prior_ensemble=prior_ens,
    )

    # Make sure we always have the same seed in updates
    rng = np.random.default_rng(42)

    if module == "IES_ENKF":
        # Step length defined as a callable on sies-iterations
        sies_step_length = functools.partial(steplength_exponential)

        # The sies-smoother is initially optional
        sies_smoother = None

        # The initial_mask equals ens_mask on first iteration
        initial_mask = prior_ens.get_realization_mask_with_responses()

        # Call an iteration of SIES algorithm. Producing snapshot and SIES obj
        iterative_smoother_update(
            prior_storage=prior_ens,
            posterior_storage=posterior_ens,
            sies_smoother=sies_smoother,
            observations=list(ert_config.observations.keys()),
            parameters=list(ert_config.ensemble_config.parameters),
            update_settings=UpdateSettings(),
            analysis_config=IESSettings(inversion="subspace_exact"),
            sies_step_length=sies_step_length,
            initial_mask=initial_mask,
            rng=rng,
        )
    else:
        smoother_update(
            prior_ens,
            posterior_ens,
            list(ert_config.observations.keys()),
            list(ert_config.ensemble_config.parameters),
            UpdateSettings(),
            ESSettings(inversion="subspace"),
            rng=rng,
        )

    sim_gen_kw = list(
        prior_ens.load_parameters("SNAKE_OIL_PARAM", 0)["values"].values.flatten()
    )

    target_gen_kw = list(
        posterior_ens.load_parameters("SNAKE_OIL_PARAM", 0)["values"].values.flatten()
    )

    # Check that prior is not equal to posterior after updationg
    assert sim_gen_kw != target_gen_kw

    # Check that posterior is as expected
    assert target_gen_kw == pytest.approx(expected_gen_kw)


@pytest.mark.usefixtures("use_tmpdir")
@pytest.mark.parametrize(
    "alpha, expected, expectation",
    [
        pytest.param(
            0.001,
            [],
            pytest.raises(ErtAnalysisError),
            id="Low alpha, no active observations",
        ),
        (
            0.1,
            [
                ObservationStatus.OUTLIER,
                ObservationStatus.OUTLIER,
                ObservationStatus.ACTIVE,
            ],
            does_not_raise(),
        ),
        (
            0.5,
            [
                ObservationStatus.OUTLIER,
                ObservationStatus.ACTIVE,
                ObservationStatus.ACTIVE,
            ],
            does_not_raise(),
        ),
        (
            1,
            [
                ObservationStatus.ACTIVE,
                ObservationStatus.ACTIVE,
                ObservationStatus.ACTIVE,
            ],
            does_not_raise(),
        ),
    ],
)
def test_smoother_snapshot_alpha(
    alpha, expected, storage, uniform_parameter, obs, expectation
):
    """
    Note that this is now a snapshot test, so there is no guarantee that the
    snapshots are correct, they are just documenting the current behavior.
    """

    # alpha is a parameter used for outlier detection

    resp = GenDataConfig(keys=["RESPONSE"])
    experiment = storage.create_experiment(
        parameters=[uniform_parameter],
        responses=[resp],
        observations={"OBSERVATION": obs},
    )
    prior_storage = storage.create_ensemble(
        experiment,
        ensemble_size=10,
        iteration=0,
        name="prior",
    )
    rng = np.random.default_rng(1234)
    for iens in range(prior_storage.ensemble_size):
        data = rng.uniform(0, 1)
        prior_storage.save_parameters(
            "PARAMETER",
            iens,
            xr.Dataset(
                {
                    "values": ("names", [data]),
                    "transformed_values": ("names", [data]),
                    "names": ["KEY_1"],
                }
            ),
        )
        data = rng.uniform(0.8, 1, 3)
        prior_storage.save_response(
            "gen_data",
            xr.Dataset(
                {"values": (["name", "report_step", "index"], [[data]])},
                coords={
                    "name": ["RESPONSE"],
                    "index": range(len(data)),
                    "report_step": [0],
                },
            ),
            iens,
        )
    posterior_storage = storage.create_ensemble(
        prior_storage.experiment_id,
        ensemble_size=prior_storage.ensemble_size,
        iteration=1,
        name="posterior",
        prior_ensemble=prior_storage,
    )

    # Step length defined as a callable on sies-iterations
    sies_step_length = functools.partial(steplength_exponential)

    # The sies-smoother is initially optional
    sies_smoother = None

    # The initial_mask equals ens_mask on first iteration
    initial_mask = prior_storage.get_realization_mask_with_responses()

    with expectation:
        result_snapshot, _ = iterative_smoother_update(
            prior_storage=prior_storage,
            posterior_storage=posterior_storage,
            sies_smoother=sies_smoother,
            observations=["OBSERVATION"],
            parameters=["PARAMETER"],
            update_settings=UpdateSettings(alpha=alpha),
            analysis_config=IESSettings(),
            sies_step_length=sies_step_length,
            initial_mask=initial_mask,
        )
        assert result_snapshot.alpha == alpha
        assert [
            step.status for step in result_snapshot.update_step_snapshots
        ] == expected


def test_update_only_using_subset_observations(
    snake_oil_case_storage, snake_oil_storage, snapshot
):
    """
    Note that this is now a snapshot test, so there is no guarantee that the
    snapshots are correct, they are just documenting the current behavior.
    """
    ert_config = snake_oil_case_storage

    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")
    posterior_ens = snake_oil_storage.create_ensemble(
        prior_ens.experiment_id,
        ensemble_size=ert_config.model_config.num_realizations,
        iteration=1,
        name="new_ensemble",
        prior_ensemble=prior_ens,
    )
    events = []

    smoother_update(
        prior_ens,
        posterior_ens,
        ["WPR_DIFF_1"],
        ert_config.ensemble_config.parameters,
        UpdateSettings(),
        ESSettings(),
        progress_callback=events.append,
    )

    update_event = next(e for e in events if isinstance(e, AnalysisCompleteEvent))
    snapshot.assert_match(
        tabulate(update_event.data.data, floatfmt=".3f") + "\n", "update_log"
    )


def test_temporary_parameter_storage_with_inactive_fields(
    storage, tmp_path, monkeypatch
):
    """
    Tests that when FIELDS with inactive cells are stored in the temporary
    parameter storage the inactive cells are not stored along with the active cells.

    Then test that we restore the inactive cells when saving the temporary
    parameter storage to disk again.
    """
    monkeypatch.chdir(tmp_path)

    num_grid_cells = 40
    layers = 5
    ensemble_size = 5
    param_group = "PARAM_FIELD"
    shape = Shape(num_grid_cells, num_grid_cells, layers)

    grid = xtgeo.create_box_grid(dimension=(shape.nx, shape.ny, shape.nz))
    mask = grid.get_actnum()
    rng = np.random.default_rng()
    mask_list = rng.choice([True, False], shape.nx * shape.ny * shape.nz)
    mask.values = mask_list
    grid.set_actnum(mask)
    grid.to_file("MY_EGRID.EGRID", "egrid")

    config = Field.from_config_list(
        "MY_EGRID.EGRID",
        shape,
        [
            param_group,
            param_group,
            "param.GRDECL",
            "INIT_FILES:param_%d.GRDECL",
            "FORWARD_INIT:False",
        ],
    )

    experiment = storage.create_experiment(
        parameters=[config],
        name="my_experiment",
    )

    prior_ensemble = storage.create_ensemble(
        experiment=experiment,
        ensemble_size=ensemble_size,
        iteration=0,
        name="prior",
    )
    fields = [
        xr.Dataset(
            {
                "values": (
                    ["x", "y", "z"],
                    np.ma.MaskedArray(
                        data=rng.random(size=(shape.nx, shape.ny, shape.nz)),
                        fill_value=np.nan,
                        mask=[~mask_list],
                    ).filled(),
                )
            }
        )
        for _ in range(ensemble_size)
    ]

    for iens in range(ensemble_size):
        prior_ensemble.save_parameters(param_group, iens, fields[iens])

    realization_list = list(range(ensemble_size))
    param_ensemble_array = _load_param_ensemble_array(
        prior_ensemble, param_group, realization_list
    )

    assert np.count_nonzero(mask_list) < (shape.nx * shape.ny * shape.nz)
    assert param_ensemble_array.shape == (
        np.count_nonzero(mask_list),
        ensemble_size,
    )

    ensemble = storage.create_ensemble(
        experiment=experiment,
        ensemble_size=ensemble_size,
        iteration=0,
        name="post",
    )

    _save_param_ensemble_array_to_disk(
        ensemble, param_ensemble_array, param_group, realization_list
    )
    for iens in range(prior_ensemble.ensemble_size):
        ds = xr.open_dataset(
            ensemble._path / f"realization-{iens}" / f"{param_group}.nc", engine="scipy"
        )
        np.testing.assert_array_equal(ds["values"].values[0], fields[iens]["values"])


def test_that_observations_keep_sorting(snake_oil_case_storage, snake_oil_storage):
    """
    The order of the observations influence the update as it affects the
    perturbations, so we make sure we maintain the order throughout.
    """
    ert_config = snake_oil_case_storage
    experiment = snake_oil_storage.get_experiment_by_name("ensemble-experiment")
    prior_ens = experiment.get_ensemble_by_name("default_0")
    assert list(ert_config.observations.keys()) == list(
        prior_ens.experiment.observations.keys()
    )


def _mock_load_observations_and_responses(
    S,
    observations,
    errors,
    obs_keys,
    indexes,
    alpha,
    std_cutoff,
    global_std_scaling,
    auto_scale_observations,
    progress_callback,
    ensemble,
):
    """
    Runs through _load_observations_and_responses with mocked values for
     _get_observations_and_responses
    """
    with patch(
        "ert.analysis._es_update._get_observations_and_responses"
    ) as mock_obs_n_responses:
        mock_obs_n_responses.return_value = (S, observations, errors, obs_keys, indexes)

        return _load_observations_and_responses(
            ensemble=ensemble,
            alpha=alpha,
            std_cutoff=std_cutoff,
            global_std_scaling=global_std_scaling,
            iens_active_index=np.array([True] * len(observations)),
            selected_observations=obs_keys,
            auto_scale_observations=auto_scale_observations,
            progress_callback=progress_callback,
        )


def test_that_autoscaling_applies_to_scaled_errors(storage):
    with patch("ert.analysis.misfit_preprocessor.main") as misfit_main:
        misfit_main.return_value = (
            np.array([2, 3]),
            np.array([1, 1]),  # corresponds to num obs keys in autoscaling group
            np.array([1, 1]),
        )

        S = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
        observations = np.array([2, 4, 3, 3])
        errors = np.array([1, 2, 1, 1])
        obs_keys = np.array(["obs1_1", "obs1_2", "obs2", "obs2"])
        indexes = np.array(["rs00", "rs0", "rs0", "rs1"])
        alpha = 1
        std_cutoff = 0.05
        global_std_scaling = 1
        progress_callback = lambda _: None

        experiment = storage.create_experiment(name="dummyexp")
        ensemble = experiment.create_ensemble(name="dummy", ensemble_size=10)
        _, (_, scaled_errors_with_autoscale, _) = _mock_load_observations_and_responses(
            S=S,
            observations=observations,
            errors=errors,
            obs_keys=obs_keys,
            indexes=indexes,
            alpha=alpha,
            std_cutoff=std_cutoff,
            global_std_scaling=global_std_scaling,
            auto_scale_observations=[["obs1*"]],
            progress_callback=progress_callback,
            ensemble=ensemble,
        )

        _, (_, scaled_errors_without_autoscale, _) = (
            _mock_load_observations_and_responses(
                S=S,
                observations=observations,
                errors=errors,
                obs_keys=obs_keys,
                indexes=indexes,
                alpha=alpha,
                std_cutoff=std_cutoff,
                global_std_scaling=global_std_scaling,
                auto_scale_observations=[],
                progress_callback=progress_callback,
                ensemble=ensemble,
            )
        )

        assert scaled_errors_with_autoscale.tolist() == [2, 6]
        assert scaled_errors_without_autoscale.tolist() == [1, 2]


def test_gen_data_obs_data_mismatch(storage, uniform_parameter):
    resp = GenDataConfig(keys=["RESPONSE"])
    obs = xr.Dataset(
        {
            "observations": (["report_step", "index"], [[1.0]]),
            "std": (["report_step", "index"], [[0.1]]),
        },
        coords={"index": [1000], "report_step": [0]},
        attrs={"response": "RESPONSE"},
    )
    experiment = storage.create_experiment(
        parameters=[uniform_parameter],
        responses=[resp],
        observations={"OBSERVATION": obs},
    )
    prior = storage.create_ensemble(
        experiment,
        ensemble_size=10,
        iteration=0,
        name="prior",
    )
    rng = np.random.default_rng(1234)
    for iens in range(prior.ensemble_size):
        data = rng.uniform(0, 1)
        prior.save_parameters(
            "PARAMETER",
            iens,
            xr.Dataset(
                {
                    "values": ("names", [data]),
                    "transformed_values": ("names", [data]),
                    "names": ["KEY_1"],
                }
            ),
        )
        data = rng.uniform(0.8, 1, 3)
        prior.save_response(
            "gen_data",
            xr.Dataset(
                {"values": (["name", "report_step", "index"], [[data]])},
                coords={
                    "name": ["RESPONSE"],
                    "index": range(len(data)),
                    "report_step": [0],
                },
            ),
            iens,
        )
    posterior_ens = storage.create_ensemble(
        prior.experiment_id,
        ensemble_size=prior.ensemble_size,
        iteration=1,
        name="posterior",
        prior_ensemble=prior,
    )
    with pytest.raises(
        ErtAnalysisError,
        match="No active observations",
    ):
        smoother_update(
            prior,
            posterior_ens,
            ["OBSERVATION"],
            ["PARAMETER"],
            UpdateSettings(),
            ESSettings(),
        )


@pytest.mark.usefixtures("use_tmpdir")
def test_gen_data_missing(storage, uniform_parameter, obs):
    resp = GenDataConfig(keys=["RESPONSE"])
    experiment = storage.create_experiment(
        parameters=[uniform_parameter],
        responses=[resp],
        observations={"OBSERVATION": obs},
    )
    prior = storage.create_ensemble(
        experiment,
        ensemble_size=10,
        iteration=0,
        name="prior",
    )
    rng = np.random.default_rng(1234)
    for iens in range(prior.ensemble_size):
        data = rng.uniform(0, 1)
        prior.save_parameters(
            "PARAMETER",
            iens,
            xr.Dataset(
                {
                    "values": ("names", [data]),
                    "transformed_values": ("names", [data]),
                    "names": ["KEY_1"],
                }
            ),
        )
        data = rng.uniform(0.8, 1, 2)  # Importantly, shorter than obs
        prior.save_response(
            "gen_data",
            xr.Dataset(
                {"values": (["name", "report_step", "index"], [[data]])},
                coords={
                    "name": ["RESPONSE"],
                    "index": range(len(data)),
                    "report_step": [0],
                },
            ),
            iens,
        )
    posterior_ens = storage.create_ensemble(
        prior.experiment_id,
        ensemble_size=prior.ensemble_size,
        iteration=1,
        name="posterior",
        prior_ensemble=prior,
    )
    events = []

    update_snapshot = smoother_update(
        prior,
        posterior_ens,
        ["OBSERVATION"],
        ["PARAMETER"],
        UpdateSettings(),
        ESSettings(),
        progress_callback=events.append,
    )
    assert [step.status for step in update_snapshot.update_step_snapshots] == [
        ObservationStatus.ACTIVE,
        ObservationStatus.ACTIVE,
        ObservationStatus.MISSING_RESPONSE,
    ]

    update_event = next(e for e in events if isinstance(e, AnalysisCompleteEvent))
    data_section = update_event.data
    assert data_section.extra["Active observations"] == "2"
    assert data_section.extra["Deactivated observations - missing respons(es)"] == "1"


@pytest.mark.usefixtures("use_tmpdir")
def test_update_subset_parameters(storage, uniform_parameter, obs):
    no_update_param = GenKwConfig(
        name="EXTRA_PARAMETER",
        forward_init=False,
        template_file="",
        transform_function_definitions=[
            TransformFunctionDefinition("KEY1", "UNIFORM", [0, 1]),
        ],
        output_file=None,
        update=False,
    )
    resp = GenDataConfig(keys=["RESPONSE"])
    experiment = storage.create_experiment(
        parameters=[uniform_parameter, no_update_param],
        responses=[resp],
        observations={"OBSERVATION": obs},
    )
    prior = storage.create_ensemble(
        experiment,
        ensemble_size=10,
        iteration=0,
        name="prior",
    )
    rng = np.random.default_rng(1234)
    for iens in range(prior.ensemble_size):
        data = rng.uniform(0, 1)
        prior.save_parameters(
            "PARAMETER",
            iens,
            xr.Dataset(
                {
                    "values": ("names", [data]),
                    "transformed_values": ("names", [data]),
                    "names": ["KEY_1"],
                }
            ),
        )
        prior.save_parameters(
            "EXTRA_PARAMETER",
            iens,
            xr.Dataset(
                {
                    "values": ("names", [data]),
                    "transformed_values": ("names", [data]),
                    "names": ["KEY_1"],
                }
            ),
        )

        data = rng.uniform(0.8, 1, 10)
        prior.save_response(
            "gen_data",
            xr.Dataset(
                {"values": (["name", "report_step", "index"], [[data]])},
                coords={
                    "name": ["RESPONSE"],
                    "index": range(len(data)),
                    "report_step": [0],
                },
            ),
            iens,
        )
    posterior_ens = storage.create_ensemble(
        prior.experiment_id,
        ensemble_size=prior.ensemble_size,
        iteration=1,
        name="posterior",
        prior_ensemble=prior,
    )
    smoother_update(
        prior,
        posterior_ens,
        ["OBSERVATION"],
        ["PARAMETER"],
        UpdateSettings(),
        ESSettings(),
    )
    assert prior.load_parameters("EXTRA_PARAMETER", 0)["values"].equals(
        posterior_ens.load_parameters("EXTRA_PARAMETER", 0)["values"]
    )
    assert not prior.load_parameters("PARAMETER", 0)["values"].equals(
        posterior_ens.load_parameters("PARAMETER", 0)["values"]
    )
