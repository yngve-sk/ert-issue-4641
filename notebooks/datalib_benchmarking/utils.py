import datetime
import json
import os
import pathlib
import time
from typing import Callable, Dict, List, Literal, Tuple, Type, TypeVar, Union

import numpy as np
import pandas as pd
import polars as pl
from dateutil.relativedelta import relativedelta
from memory_profiler import memory_usage
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks, Stats

p_out = pathlib.Path(os.getcwd()) / "benchmark_data_processing_out"


def p_real(realization: int) -> pathlib.Path:
    return p_out / f"realization-{realization}"


def create_realization_dirs(num_reals: int):
    for i in range(num_reals):
        os.makedirs(f"benchmark_data_processing_out/realization-{i}", exist_ok=True)


def gen_data_keylist(num_keys: int):
    return [f"gd_{gdi}" for gdi in range(num_keys)]


def summary_keylist(num_keys: int):
    return [f"smry_{i}" for i in range(num_keys)]


def fill_realization_dirs_with_initial_data(
    libs_to_test: Dict[str, Type[DatalibHooks]],
    num_reals: int,
    num_gen_data_keys: int,
    gen_data_index_list: List[int],
    gen_data_report_step_list: List[int],
    num_summary_keys: int,
    num_summary_timesteps: int,
) -> Dict[str, List[Stats]]:
    all_filesave_stats: Dict[str, Stats] = {k: [] for k in libs_to_test}
    failing_realizations = {0, 5, 10}
    realizations_with_some_missing_response_keys = {0, 2, 6, 11}
    for i in range(num_reals):
        if i in failing_realizations:
            continue

        print(f"Filling dir for realization {i}")

        _num_gen_data_keys = num_gen_data_keys - (
            0 if i not in realizations_with_some_missing_response_keys else 2
        )
        _num_summary_keys = num_summary_keys - (
            0 if i not in realizations_with_some_missing_response_keys else 2
        )

        gen_data_keys = gen_data_keylist(_num_gen_data_keys)
        gen_data_df = pd.DataFrame(
            data={
                "index": gen_data_index_list * len(gen_data_report_step_list),
                "report_step": np.repeat(
                    gen_data_report_step_list, len(gen_data_index_list)
                ),
                "value": list(
                    range(len(gen_data_report_step_list) * len(gen_data_index_list))
                ),
            }
        ).set_index(["index", "report_step"], verify_integrity=True)

        for gen_data_key in gen_data_keys:
            for datalib_name, hooks in libs_to_test.items():
                _filename = p_real(i) / f"{gen_data_key}.{datalib_name}"
                if os.path.exists(_filename):
                    continue

                print(f"Saving gen data ds {_filename}")
                filesave_stats = hooks.from_dataframe_to_file(
                    gen_data_df, str(_filename)
                )

                if datalib_name not in all_filesave_stats:
                    all_filesave_stats[datalib_name] = []

                if filesave_stats is not None:
                    all_filesave_stats[datalib_name].append(filesave_stats)

        all_response_keys = summary_keylist(_num_summary_keys)
        all_timesteps = [
            f"{(datetime.datetime(2000, 1, 1) + relativedelta(months=i))}"
            for i in range(num_summary_timesteps)
        ]

        smry_df = pd.DataFrame(
            data={
                "response_key": np.repeat(all_response_keys, len(all_timesteps)),
                "time": np.tile(all_timesteps, len(all_response_keys)),
                "value": list(range(_num_summary_keys * num_summary_timesteps)),
            }
        ).set_index(["response_key", "time"], verify_integrity=True)

        for datalib_name, hooks in libs_to_test.items():
            _filename = p_real(i) / f"summary.{datalib_name}"
            print(f"Saving summary ds to {_filename}")
            filesave_stats = hooks.from_dataframe_to_file(smry_df, str(_filename))

            if filesave_stats is not None:
                all_filesave_stats[datalib_name].append(filesave_stats)
                df = hooks.from_file_to_dataframe(_filename)

    return all_filesave_stats


def create_observations(
    n_observations: int,
    responses_ds: pl.DataFrame,
    observed_keys: Union[None, List[str], Literal["*"]] = "*",
) -> pl.DataFrame:
    if observed_keys != "*" and observed_keys is not None:
        responses_ds = responses_ds.filter(pl.col("response_key").is_in(observed_keys))

    observed_response_keys = responses_ds.select(["response_key", "time"]).sample(
        n=n_observations, with_replacement=True
    )

    rng = np.random.default_rng()
    observations_ds = pl.DataFrame(
        {
            "obs_key": [f"obs_{i}" for i in range(n_observations)],
            "observation": rng.normal(10, scale=1, size=n_observations),
            "error": rng.normal(1, scale=0.1, size=n_observations),
        }
    )

    return observed_response_keys.hstack(observations_ds)


def save_timings_list_to_file(timings: Dict[str, List[Stats]], filepath: str):
    with open(filepath, "w") as f:
        json.dump(
            {
                k: [my_stats.to_dict() for my_stats in stats_list]
                for k, stats_list in timings.items()
            },
            f,
        )


def read_timings_list_from_file(filepath: str) -> Dict[str, List[Stats]]:
    with open(filepath, "r") as f:
        the_dict = json.load(f)
        return {
            k: [Stats.from_dict(d) for d in stats_list]
            for k, stats_list in the_dict.items()
        }


def save_timings_to_file(timings: Dict[str, Stats], filepath: str):
    with open(filepath, "w") as f:
        json.dump(
            {k: stats.to_dict() for k, stats in timings.items() if stats is not None},
            f,
        )


def read_timings_from_file(filepath: str) -> Dict[str, Stats]:
    with open(filepath, "r") as f:
        the_dict = json.load(f)
        return {k: Stats.from_dict(stats_dict) for k, stats_dict in the_dict.items()}


T = TypeVar("T")


def profile_me(func: Callable[..., T]) -> Callable[..., Tuple[Stats, T]]:
    def wrapper(*args, **kwargs):
        t0 = time.time()

        max_usage_before = max(memory_usage(include_children=True))
        time.sleep(5)
        mem_usage, result = memory_usage(
            (func, args, kwargs), retval=True, include_children=True
        )

        return result, Stats(
            time_seconds=time.time() - t0,
            memory_usage_MiB=max(mem_usage) - max_usage_before,
        )

    return wrapper


def profile_me_stats_only(func: Callable) -> Callable[..., Stats]:
    def wrapper(*args, **kwargs):
        t0 = time.time()
        max_usage_before = max(memory_usage(include_children=True))
        time.sleep(5)
        mem_usage = memory_usage(
            (func, args, kwargs), retval=False, include_children=True
        )

        return Stats(
            time_seconds=time.time() - t0,
            memory_usage_MiB=max(mem_usage) - max_usage_before,
        )

    return wrapper
