import os.path
from typing import Dict, Type

import polars as pl
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks
from notebooks.datalib_benchmarking.plotting import plot_timing_lists, plot_timings
from notebooks.datalib_benchmarking.utils import (
    create_observations,
    create_realization_dirs,
    fill_realization_dirs_with_initial_data,
    gen_data_keylist,
    p_out,
    p_real,
    read_timings_from_file,
    read_timings_list_from_file,
    save_timings_list_to_file,
    save_timings_to_file,
    summary_keylist,
)

num_reals = 2

num_observations = 20
num_obs_keys = 50

num_summary_keys = 5
num_summary_timesteps = 100

# Note: The gen data datasets here are DENSE
# i.e., there is a value for every cell, so
# xarray should do well with this
num_gen_data_keys = 5
gen_data_index_list = [1000, 1800, 2000]
gen_data_report_step_list = list(range(2))

repr_gendata = f"{{n_gen_data_keys:{num_gen_data_keys},num_index:{len(gen_data_index_list)},num_report_steps:{len(gen_data_report_step_list)}}} across {num_reals} realizations"
repr_summary = f"{{num_summary_keys:{num_summary_keys},num_summary_timesteps:{num_summary_timesteps} across {num_reals} realizations"
n_summary_rows = num_summary_keys * num_summary_timesteps

from notebooks.datalib_benchmarking.datalib_spec_arrow import ArrowHooks
from notebooks.datalib_benchmarking.datalib_spec_duckdb import DuckdbHooks
from notebooks.datalib_benchmarking.datalib_spec_polars import PolarsHooks
from notebooks.datalib_benchmarking.datalib_spec_polars_parquet import (
    PolarsHooksParquet,
)
from notebooks.datalib_benchmarking.datalib_spec_xarray import XRHooks

stuff_to_run = {
    "create_real_dirs",
    "verify_datasets_equality",
    "combine_gen_data",
    "combine_summary",
    "query_all_realizations_with_all_responses",
    "query_all_single_realization_for_all_responses",
    "query_single_realization_for_single_summary_key",
    "create_observations_for_combined_summary",
    "join_with_summary_observations",
    "query_single_summary_key_for_all_realizations",
}

# 4convenience
plotting_to_run = {
    "plot_save_timings",
    "plot_combine_gen_data",
    "plot_combine_summary",
    "plot_query_all_realizations_with_all_responses",
    "plot_query_all_single_realization_for_all_responses",
    "plot_query_single_realization_for_single_summary_key",
    "plot_query_single_summary_key_for_all_realizations",
    "plot_join_with_summary_observations",
}

steps_to_run = {*plotting_to_run, *stuff_to_run}

libs_to_test: Dict[str, Type[DatalibHooks]] = {
    "xarray": XRHooks,
    "polars": PolarsHooks,
    "arrow": ArrowHooks,
    "duckdb": DuckdbHooks,
    "polars_parquet": PolarsHooksParquet,
}

if __name__ == "__main__":
    if "create_real_dirs" in steps_to_run:
        create_realization_dirs(num_reals)
        all_filesave_stats = fill_realization_dirs_with_initial_data(
            libs_to_test=libs_to_test,
            num_reals=num_reals,
            num_gen_data_keys=num_gen_data_keys,
            gen_data_index_list=gen_data_index_list,
            gen_data_report_step_list=gen_data_report_step_list,
            num_summary_keys=num_summary_keys,
            num_summary_timesteps=num_summary_timesteps,
        )

        save_timings_list_to_file(
            all_filesave_stats, p_out / "save_to_file_timings.json"
        )

    if "verify_datasets_equality" in steps_to_run:
        gen_data_keys = gen_data_keylist(num_gen_data_keys)
        summary_datasets = {name: {} for name in libs_to_test}

        gen_data_datasets = {
            name: {k: {} for k in gen_data_keys} for name in libs_to_test
        }
        for i in range(num_reals):
            for name, hooks in libs_to_test.items():
                for k in gen_data_keys:
                    fp = p_real(i) / f"{k}.{name}"

                    if os.path.exists(fp):
                        _df = hooks.from_file_to_dataframe(str(fp))

                        if _df is not None:
                            gen_data_datasets[name][k][i] = _df

            for name, hooks in libs_to_test.items():
                fp = p_real(i) / f"summary.{name}"

                if os.path.exists(fp):
                    df = hooks.from_file_to_dataframe(str(fp))

                    if df is not None:
                        summary_datasets[name][i] = df

        for i in range(num_reals):
            all_summary_ds = {
                name: summary_datasets[name][i]
                for name in libs_to_test
                if (summary_datasets[name] is not None and i in summary_datasets[name])
            }

            if len(all_summary_ds) > 1:
                _smry_ds_list = [_ds for _ds, _ in all_summary_ds.values()]
                first = _smry_ds_list[0]
                for other in _smry_ds_list[1:]:
                    assert (
                        first.set_index(["response_key", "time"])
                        .sort_index()
                        .equals(other.set_index(["response_key", "time"]).sort_index())
                    )

        for _i in range(num_reals):
            pass
        print("hehe")

    if "plot_save_timings" in steps_to_run:
        timings_list_per_lib = read_timings_list_from_file(
            p_out / "save_to_file_timings.json"
        )
        plot_timing_lists(
            timings_list_per_lib,
            title_time=(
                f"Time (seconds) to save single "
                f"dataset to file across {num_reals} realizations"
            ),
            title_mem=(
                f"Memory used (MiB) to save single "
                f"dataset to file across {num_reals} realizations"
            ),
            title_size=("File size of dataset files"),
        )

    if "plot_file_sizes" in steps_to_run:
        # Find realization dir with most files
        # Remove suffixes (split by ".")
        # Make dotplot, one dot per filename, over cols
        pass

    if "combine_gen_data" in steps_to_run:
        _all_combine_stats = {name: [] for name in libs_to_test}
        for name, hooks in libs_to_test.items():
            _combine_stats = hooks.combine_one_ds_per_response_key(
                response_keys=gen_data_keylist(num_gen_data_keys), num_reals=num_reals
            )

            _all_combine_stats[name] = _combine_stats

        save_timings_to_file(
            _all_combine_stats,
            p_out / "combine_gen_data_timings.json",
        )

    if "plot_combine_gen_data" in steps_to_run:
        timings = read_timings_from_file(p_out / "combine_gen_data_timings.json")
        plot_timings(
            timings,
            title_time=f"Time (seconds) to combine gen data {repr_gendata}",
            title_mem=f"Memory (MiB) to combine gen data {repr_gendata}",
        )

    if "combine_summary" in steps_to_run:
        _all_combine_stats = {name: [] for name in libs_to_test}
        for name, hooks in libs_to_test.items():
            _combine_stats = hooks.combine_one_ds_many_response_keys(
                f"summary.{name}", num_reals=num_reals
            )

            _all_combine_stats[name] = _combine_stats
        save_timings_to_file(_all_combine_stats, p_out / "combine_summary_timings.json")

    if "plot_combine_summary" in steps_to_run:
        timings = read_timings_from_file(p_out / "combine_summary_timings.json")
        plot_timings(
            timings,
            title_time=f"Time (seconds) to combine summary {repr_summary}",
            title_mem=f"Memory (MiB) to combine summary {repr_summary}",
        )

    if "query_all_realizations_with_all_responses" in steps_to_run:
        _query_results = {}
        _query_timings = {}

        for name, hooks in libs_to_test.items():
            result = hooks.query_all_realizations_with_all_responses(
                str(p_out / f"summary_combined.{name}")
            )

            if result is not None:
                _reals_with_all_responses, _timings = result

                _query_timings[name] = _timings
                _query_results[name] = _reals_with_all_responses

        save_timings_to_file(
            _query_timings,
            p_out / "query_all_realizations_with_all_responses.json",
        )

    if "plot_query_all_realizations_with_all_responses" in steps_to_run:
        timings = read_timings_from_file(
            p_out / "query_all_realizations_with_all_responses.json"
        )
        plot_timings(
            timings,
            title_time=f"Time (seconds) to query all reals w/ all responses\nfrom summary {repr_summary}",
            title_mem=f"Memory (MiB) to query all reals w/ all responses\nfrom summary {repr_summary}",
        )

    if "query_all_single_realization_for_all_responses" in steps_to_run:
        _query_results = {}
        _query_timings = {}

        for name, hooks in libs_to_test.items():
            result = hooks.query_single_realization_for_all_existing_responses(
                str(p_out / f"summary_combined.{name}"), 1
            )

            if result is not None:
                _reals_with_all_responses, _timings = result

                _query_timings[name] = _timings
                _query_results[name] = _reals_with_all_responses

        save_timings_to_file(
            _query_timings,
            p_out / "query_single_realization_for_all_responses.json",
        )

    if "plot_query_all_single_realization_for_all_responses" in steps_to_run:
        timings = read_timings_from_file(
            p_out / "query_single_realization_for_all_responses.json"
        )
        plot_timings(
            timings,
            title_time=f"Time (seconds) to query single real for all existing responses\nfrom summary {repr_summary}",
            title_mem=f"Memory (MiB) to query single real for all existing responses\nfrom summary {repr_summary}",
        )

    if "query_single_realization_for_single_summary_key" in steps_to_run:
        _query_results = {}
        _query_timings = {}
        sample_smry_key = summary_keylist(num_summary_keys)[2]

        for name, hooks in libs_to_test.items():
            result = hooks.query_single_summary_key_for_one_realization(
                str(p_out / f"summary_combined.{name}"),
                response_key=sample_smry_key,
                realization=1,
            )

            if result is not None:
                _reals_with_all_responses, _timings = result

                _query_timings[name] = _timings
                _query_results[name] = _reals_with_all_responses

        save_timings_to_file(
            _query_timings,
            p_out / "query_single_realization_for_single_summary_key.json",
        )

    if "plot_query_single_realization_for_single_summary_key" in steps_to_run:
        timings = read_timings_from_file(
            p_out / "query_single_realization_for_single_summary_key.json"
        )
        plot_timings(
            timings,
            title_time=f"Time (seconds) to query summary data for one response key+realization\nfrom summary {repr_summary}",
            title_mem=f"Memory (MiB) to query summary data for one response key+realization\nfrom summary {repr_summary}",
        )

    if "query_single_summary_key_for_all_realizations" in steps_to_run:
        _query_results = {}
        _query_timings = {}
        sample_smry_key = summary_keylist(num_summary_keys)[2]

        for name, hooks in libs_to_test.items():
            result = hooks.query_single_summary_key_for_all_realizations(
                str(p_out / f"summary_combined.{name}"),
                response_key=sample_smry_key,
            )

            if result is not None:
                _reals_with_all_responses, _timings = result

                _query_timings[name] = _timings
                _query_results[name] = _reals_with_all_responses

        save_timings_to_file(
            _query_timings,
            p_out / "query_single_summary_key_for_all_realizations.json",
        )

    if "plot_query_single_summary_key_for_all_realizations" in steps_to_run:
        timings = read_timings_from_file(
            p_out / "query_single_summary_key_for_all_realizations.json"
        )
        plot_timings(
            timings,
            title_time=f"Time (seconds) to query summary data for one response key for {num_reals} realizations\nfrom summary {repr_summary}",
            title_mem=f"Memory (MiB) to query summary data for one response key for {num_reals} realizations\nfrom summary {repr_summary}",
        )

    if "create_observations_for_combined_summary" in steps_to_run:
        smry = pl.read_ipc(p_out / "summary_combined.polars")
        summary_obs_pl = create_observations(
            n_observations=num_observations,
            responses_ds=smry,
            observed_keys="*",
        )
        summary_obs_pl.write_ipc(p_out / "summary_obs.arrow")
        summary_obs_pl.write_parquet(p_out / "summary_obs.parquet")
        pddf = (
            summary_obs_pl.to_pandas()
            .reset_index()
            .drop("index", axis=1)
            .set_index(["obs_key", "response_key", "time"], verify_integrity=True)
        )
        xrds = pddf.to_xarray()
        xrds.to_netcdf(p_out / "summary_obs.xarray")

    if "join_with_summary_observations" in steps_to_run:
        _query_results = {}
        _query_timings = {}

        for name, hooks in libs_to_test.items():
            result = hooks.join_observations_and_responses(
                responses_ds_file=str(p_out / f"summary_combined.{name}"),
                observations_file=str(p_out / f"summary_obs.{name}"),
                primary_key=["time"],
            )

            if result is not None:
                _reals_with_all_responses, _timings = result

                _query_timings[name] = _timings
                _query_results[name] = _reals_with_all_responses

        save_timings_to_file(
            _query_timings,
            p_out / "join_with_summary_observations.json",
        )

    if "plot_join_with_summary_observations" in steps_to_run:
        timings = read_timings_from_file(p_out / "join_with_summary_observations.json")
        plot_timings(
            timings,
            title_time=f"Time (seconds) to join summary data ({n_summary_rows} rows) with {num_observations} observations\nfrom summary {repr_summary}",
            title_mem=f"Memory (MiB) to join summary data ({n_summary_rows} rows) with {num_observations} observations\nfrom summary {repr_summary}",
        )
