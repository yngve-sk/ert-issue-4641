import json
from datetime import datetime, timedelta
from typing import Any, Dict

import orjson
import polars
import pytest

from ert.config import SummaryConfig
from ert.run_models.everest_run_model import EverestRunModel
from everest.api import EverestDataAPI
from everest.config import EverestConfig


# Utility to round all floats in an arbitrary json object
# (without tuples)
def _round_floats(obj, dec):
    if isinstance(obj, float):
        return round(obj, dec)
    if isinstance(obj, dict):
        return {k: _round_floats(v, dec) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_round_floats(item, dec) for item in obj]
    # Assume no tuples
    return obj


def make_api_snapshot(api) -> Dict[str, Any]:
    api_json = {
        "batches": api.batches,
        "control_names": api.control_names,
        "accepted_batches": api.accepted_batches,
        "objective_function_names": api.objective_function_names,
        "output_constraint_names": api.output_constraint_names,
        "realizations": api.realizations,
        "simulations": api.simulations,
        "control_values": api.control_values,
        "single_objective_values": api.single_objective_values,
        "gradient_values": api.gradient_values,
        **{
            f"input_constraint('{control}')": api.input_constraint(control)
            for control in api.control_names
        },
        **{
            f"output_constraint('{constraint}')": api.output_constraint(constraint)
            for constraint in api.output_constraint_names
        },
    }

    return api_json


@pytest.mark.parametrize(
    "config_file",
    [
        "config_advanced.yml",
        "config_minimal.yml",
        "config_multiobj.yml",
        "config_auto_scaled_controls.yml",
        "config_cvar.yml",
        "config_discrete.yml",
        "config_stddev.yml",
    ],
)
def test_api_snapshots(
    config_file,
    copy_math_func_test_data_to_tmp,
    evaluator_server_config_generator,
    snapshot,
):
    config = EverestConfig.load_file(config_file)
    run_model = EverestRunModel.create(config)
    evaluator_server_config = evaluator_server_config_generator(run_model)
    run_model.run_experiment(evaluator_server_config)

    optimal_result = run_model.result
    optimal_result_json = {
        "batch": optimal_result.batch,
        "controls": optimal_result.controls,
        "total_objective": optimal_result.total_objective,
    }

    api = EverestDataAPI(config)
    json_snapshot = make_api_snapshot(api)
    json_snapshot["optimal_result_json"] = optimal_result_json
    rounded_json_snapshot = _round_floats(json_snapshot, 8)

    snapshot_str = (
        orjson.dumps(rounded_json_snapshot, option=orjson.OPT_INDENT_2)
        .decode("utf-8")
        .strip()
        + "\n"
    )
    snapshot.assert_match(snapshot_str, "snapshot.json")


def test_api_summary_snapshot(
    copy_math_func_test_data_to_tmp, evaluator_server_config_generator, snapshot
):
    config = EverestConfig.load_file("config_minimal.yml")
    run_model = EverestRunModel.create(config)
    evaluator_server_config = evaluator_server_config_generator(run_model)
    run_model.run_experiment(evaluator_server_config)

    # Save some summary data to each ensemble
    experiment = next(run_model._storage.experiments)

    response_config = experiment.response_configuration
    response_config["summary"] = SummaryConfig(keys=["*"])

    experiment._storage._write_transaction(
        experiment._path / experiment._responses_file,
        json.dumps(
            {c.response_type: c.to_dict() for c in response_config.values()},
            default=str,
            indent=2,
        ).encode("utf-8"),
    )

    smry_data = polars.DataFrame(
        {
            "response_key": ["FOPR", "FOPR", "WOPR", "WOPR", "FOPT", "FOPT"],
            "time": polars.Series(
                [datetime(2000, 1, 1) + timedelta(days=i) for i in range(6)]
            ).dt.cast_time_unit("ms"),
            "values": polars.Series(
                [0.2, 0.2, 1.0, 1.1, 3.3, 3.3], dtype=polars.Float32
            ),
        }
    )
    for ens in experiment.ensembles:
        for real in range(ens.ensemble_size):
            ens.save_response("summary", smry_data.clone(), real)

    api = EverestDataAPI(config)
    dicts = polars.from_pandas(api.summary_values()).to_dicts()
    snapshot.assert_match(
        orjson.dumps(dicts, option=orjson.OPT_INDENT_2).decode("utf-8").strip() + "\n",
        "snapshot.json",
    )
