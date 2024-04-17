import datetime
import pathlib
import random
from functools import lru_cache

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from ert.config import GenDataConfig, SummaryConfig
from ert.storage import LocalEnsemble, open_storage
from ert.storage.local_ensemble import InMemoryStorageForSmallDatasets


def _create_gen_data_config_ds_and_obs(
    num_gen_data, num_gen_obs, num_indices, num_report_steps
):
    gen_data_configs = [
        *[GenDataConfig(name=f"gen_data_{i}") for i in range(num_gen_data)]
    ]

    gen_data_ds = {
        f"{gen_data_configs[i].name}": pd.DataFrame(
            data={
                "index": [(j % num_indices) for j in range(num_report_steps)],
                "report_step": list(range(num_report_steps)),
                "values": [random.random() * 10 for _ in range(num_report_steps)],
            }
        )
        .set_index(["index", "report_step"])
        .to_xarray()
        for i in range(num_gen_data)
    }

    gen_data_obs = (
        pd.DataFrame(
            data={
                "name": [
                    gen_data_configs[(i % num_gen_data)].name
                    for i in range(num_gen_obs)
                ],
                "obs_name": [f"gen_obs_{i}" for i in range(num_gen_obs)],
                "index": [
                    f"{random.randint(0,num_indices)}" for _ in range(num_gen_obs)
                ],
                "report_step": [
                    random.randint(0, num_report_steps) for _ in range(num_gen_obs)
                ],
                "observations": [random.uniform(-100, 100) for _ in range(num_gen_obs)],
                "std": [random.uniform(0, 1) for _ in range(num_gen_obs)],
            }
        )
        .set_index(["name", "obs_name", "index", "report_step"])
        .to_xarray()
    )

    return gen_data_configs, gen_data_ds, gen_data_obs


def _create_summary_config_ds_and_obs(
    num_summary_names, num_summary_timesteps, num_summary_obs
):
    summary_config = SummaryConfig(
        name="summary",
        keys=[f"sum_key_{i}" for i in range(num_summary_names)],
        input_file="",
    )

    summary_df = pd.DataFrame(
        data={
            "time": [
                pd.to_datetime(
                    datetime.date(2010, 1, 1)
                    + datetime.timedelta(days=10 * (i // num_summary_names))
                )
                for i in range(num_summary_names * num_summary_timesteps)
            ],
            "name": [
                f"sum_key_{i%num_summary_names}"
                for i in range(num_summary_names * num_summary_timesteps)
            ],
            "values": [i for i in range(num_summary_names * num_summary_timesteps)],
        }
    )

    summary_obs_ds = (
        pd.DataFrame(
            data={
                "time": [summary_df.loc[i]["time"] for i in range(num_summary_obs)],
                "name": [summary_df.loc[i]["name"] for i in range(num_summary_obs)],
                "obs_name": [f"sum_obs_{i}" for i in range(num_summary_obs)],
                "observations": [
                    random.uniform(-100, 100) for _ in range(num_summary_obs)
                ],
                "std": [random.uniform(0, 1) for _ in range(num_summary_obs)],
            }
        )
        .set_index(["name", "obs_name", "time"])
        .to_xarray()
    )

    summary_ds = summary_df.set_index(["name", "time"]).to_xarray()

    return summary_config, summary_ds, summary_obs_ds


@pytest.mark.usefixtures("use_tmpdir")
@pytest.mark.parametrize(
    (
        "num_reals, num_gen_data, num_gen_obs, num_indices, num_report_steps, in_memory_storage_max"
    ),
    [
        (100, 1, 1, 1, 1, int(1e9)),
        (100, 5, 3, 2, 10, int(1e9)),
        (200, 200, 100, 10, 200, int(1e9)),
        (100, 1, 1, 1, 1, 0),
        (50, 50, 25, 10, 30, 0),
    ],
)
def test_unify_gen_data_correctness(
    tmpdir,
    num_reals,
    num_gen_data,
    num_gen_obs,
    num_indices,
    num_report_steps,
    in_memory_storage_max,
):
    gen_data_configs, gen_data_ds, _ = _create_gen_data_config_ds_and_obs(
        num_gen_data, num_gen_obs, num_indices, num_report_steps
    )

    LocalEnsemble.IN_MEMORY_STORAGE_MAX_SIZE = in_memory_storage_max
    with open_storage(tmpdir, "w") as s:
        exp = s.create_experiment(
            responses=[*gen_data_configs],
        )

        ens = exp.create_ensemble(ensemble_size=num_reals, name="zero")
        for group, ds in gen_data_ds.items():
            for i in range(num_reals):
                ens.save_response(group, ds, i)

        ens.unify_responses("gen_data")

        combined = ens.load_responses("gen_data", realizations=tuple(range(num_reals)))

        by_group = []
        for group, ds in gen_data_ds.items():
            by_group.append(ds.expand_dims(name=[group]))

        ds_by_name = xr.concat(by_group, dim="name")
        by_real = []
        for i in range(num_reals):
            by_real.append(ds_by_name.expand_dims(realization=[i]))

        assert (
            xr.concat(by_real, dim="realization")
            .sortby("name")
            .equals(combined.sortby("name"))
        )


@pytest.mark.usefixtures("use_tmpdir")
@pytest.mark.parametrize(
    (
        "num_reals, num_summary_names, num_summary_timesteps, num_summary_obs, in_memory_storage_max"
    ),
    [
        (2, 2, 2, 1, 0),
        (100, 10, 200, 1, 0),
        (2, 2, 2, 1, 1e8),
        (100, 10, 200, 1, int(1e8)),
        (2, 2, 2, 1, 1e9),
        (500, 10, 2000, 1, 1e9),
    ],
)
def test_unify_summary_correctness(
    tmpdir,
    num_reals,
    num_summary_names,
    num_summary_timesteps,
    num_summary_obs,
    in_memory_storage_max,
):
    summary_config, summary_ds, _ = _create_summary_config_ds_and_obs(
        num_summary_names, num_summary_timesteps, num_summary_obs
    )

    LocalEnsemble.IN_MEMORY_STORAGE_MAX_SIZE = in_memory_storage_max
    with open_storage(tmpdir, "w") as s:
        exp = s.create_experiment(responses=[summary_config])

        ens = exp.create_ensemble(ensemble_size=num_reals, name="zero")
        for i in range(num_reals):
            ens.save_response("summary", summary_ds, i)

        ens.unify_responses("summary")

        combined = ens.load_responses("summary", realizations=tuple(range(num_reals)))

        manual_concat = []
        for i in range(num_reals):
            manual_concat.append(summary_ds.expand_dims(realization=[i]))

        assert combined.equals(
            xr.combine_nested(manual_concat, concat_dim="realization")
        )


num_floats_per_ds = [1, 10, 100, 1000, 10000, 20000, 50000, 100000, 200000]


@lru_cache
def _create_mock_ds(realization: int, num_vals: int):
    return xr.Dataset(
        {
            "values": (
                ["realization", "argh"],
                np.arange(num_vals).reshape((1, -1)).astype(float),
            ),
        },
        coords={
            "realization": [realization],
        },
    )


@pytest.mark.usefixtures("use_tmpdir")
@pytest.mark.parametrize(
    "num_reals, num_floats_per_group, max_memory_size_per_ds, max_memory_size_total",
    [
        (100, num_floats_per_ds, 1e3, 1e9),
        (100, num_floats_per_ds, 1e4, 1e9),
        (100, num_floats_per_ds, 1e3, 1e2),
        (100, num_floats_per_ds, 1e1, 1e2),
        (100, num_floats_per_ds, 1e2, 1e2),
        (100, num_floats_per_ds, 1e4, 1e2),
    ],
)
def test_inmemory_storage_correctness(
    tmpdir,
    num_reals,
    num_floats_per_group,
    max_memory_size_per_ds,
    max_memory_size_total,
):
    # Mocked fixed-size ds will fail if we enable consolidating&writing as we go
    smalls = InMemoryStorageForSmallDatasets(
        int(max_memory_size_total), int(max_memory_size_per_ds), tmpdir
    )

    memory_used = 0
    for i, num_floats_in_ds in enumerate(num_floats_per_group):
        group_name = f"group_{i}"
        for real in range(num_reals):
            mock_ds = _create_mock_ds(real, int(num_floats_in_ds))
            memory_used_if_stored = memory_used + mock_ds.nbytes
            # mock_ds = MockXRDataset(nbytes=num_floats_in_ds)
            did_store = smalls.try_store_in_memory(group_name, real, mock_ds)

            if mock_ds.nbytes > max_memory_size_per_ds:
                assert smalls.nbytes == memory_used
                assert not did_store
                mock_ds.to_netcdf(f"ds_g:{group_name}_i:{real}")
            elif memory_used_if_stored > max_memory_size_total:
                assert did_store
                memory_used = mock_ds.nbytes
                assert smalls.nbytes == memory_used
            else:
                assert did_store
                assert smalls.nbytes == memory_used_if_stored
                memory_used = memory_used_if_stored

    # Remove ds for one group at a time
    for i, _ in enumerate(num_floats_per_group):
        group_name = f"group_{i}"

        datasets_in_memory = smalls.consume_group_if_exists(group_name)
        datasets_in_files = pathlib.Path(tmpdir).glob(f"ds_g:{group_name}*")

        datasets_all = [
            *[xr.open_dataset(p) for p in datasets_in_files],
            *datasets_in_memory,
        ]
        recombined = xr.combine_nested(datasets_all, concat_dim="realization")

        assert len(recombined["realization"]) == num_reals

    assert smalls.nbytes == 0
