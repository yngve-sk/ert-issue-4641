import os
import time
from typing import List, Optional, Set, Tuple, Union

import pandas as pd
import polars as pl
from memory_profiler import memory_usage
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks, Stats
from notebooks.datalib_benchmarking.utils import (
    p_out,
    p_real,
    profile_me,
    profile_me_stats_only,
)


class PolarsHooks(DatalibHooks):
    @staticmethod
    @profile_me
    def from_file_to_dataframe(filename: str) -> Optional[Tuple[pd.DataFrame, Stats]]:
        return pl.read_ipc(filename).to_pandas()

    @staticmethod
    @profile_me
    def query_single_summary_key_for_all_realizations(
        ds_file: str, response_key: str
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        if os.path.exists(ds_file):
            ds = pl.read_ipc(ds_file)
            pddf = ds.filter((pl.col("response_key") == response_key)).select(
                ["realization", "time", "value"]
            )

            return pddf

    @staticmethod
    @profile_me
    def join_observations_and_responses(
        responses_ds_file: str, observations_file: str, primary_key: List[str]
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        responses_ds = pl.read_ipc(responses_ds_file.replace(".polars", ".arrow"))
        obs_ds = pl.read_ipc(observations_file.replace(".polars", ".arrow"))
        responses_pvt = responses_ds.pivot(values="value", on="realization")
        joined = obs_ds.join(
            responses_pvt, on=["response_key", *primary_key], how="left"
        )
        return joined

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_per_response_key(
        response_keys: List[str], num_reals: int
    ) -> Optional[Stats]:
        all_ds = []
        for response_key in response_keys:
            for i in range(num_reals):
                fp = f"{p_real(i)}/{response_key}.polars"
                if os.path.exists(fp):
                    ds = pl.read_ipc(f"{p_real(i)}/{response_key}.polars").with_columns(
                        [
                            pl.lit(i).alias("realization"),
                            pl.lit(response_key).alias("response_key"),
                        ]
                    )
                    all_ds.append(ds)

        concatd = pl.concat(all_ds)
        concatd.write_ipc(p_out / "gen_data_combined.polars")

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_many_response_keys(
        ds_file: str, num_reals: int
    ) -> Optional[Stats]:
        all_ds = []
        for i in range(num_reals):
            fp = f"{p_real(i)}/{ds_file}"
            if os.path.exists(fp):
                ds = pl.read_ipc(fp).with_columns([pl.lit(i).alias("realization")])
                all_ds.append(ds)

        concatd = pl.concat(all_ds)
        concatd.write_ipc(p_out / "summary_combined.polars")

    @staticmethod
    @profile_me
    def query_single_realization_for_all_existing_responses(
        ds_file: str,
        realization: int,
    ) -> Optional[Tuple[Set[str], Stats]]:
        # Insert logic here
        # 1. Filter out for realization
        # 2. Find set of keys in remaining ds
        # 3. Check that this set of keys corresponds to the set of keys from all ds
        # Read the file into a Polars DataFrame
        df = pl.read_ipc(ds_file)

        # Filter the DataFrame where realization == 1
        filtered_df = df.filter(pl.col("realization") == realization)

        # Find all unique values of the response_key column within the filtered DataFrame
        unique_response_names_list = (
            filtered_df.select("response_key").unique().to_series().to_list()
        )

        # Convert to a set for easier handling (optional)
        unique_response_names_set = set(unique_response_names_list)

        return unique_response_names_set

    @staticmethod
    @profile_me
    def query_all_realizations_with_all_responses(
        ds_file: str,
    ) -> Optional[Tuple[Set[int], Stats]]:
        df = pl.read_ipc(ds_file, columns=["realization", "response_key"])

        response_key_count = df.group_by("realization").agg(
            pl.col("response_key").n_unique().alias("response_key_count")
        )

        # Step 2: Calculate the total number of distinct response_key
        total_keys = df.select(
            pl.col("response_key").n_unique().alias("total_response_keys")
        )

        # Step 3: Filter realizations with response_key_count equal to total_response_keys
        result = response_key_count.filter(
            pl.col("response_key_count") == total_keys["total_response_keys"][0]
        ).select("realization")

        return set(result["realization"].to_list())

    @staticmethod
    def from_dataframe_to_file(
        df: pd.DataFrame, output_filename: str
    ) -> Optional[Stats]:
        # Convert pandas DataFrame to Polars DataFrame
        polars_df = pl.from_pandas(df.reset_index())

        t0 = time.time()
        mem_usage_before = memory_usage(-1, interval=0.01, timeout=0.1)
        polars_df.write_ipc(file=output_filename, compression="uncompressed")
        t1 = time.time()
        mem_usage_after = memory_usage(-1, interval=0.01, timeout=0.1)
        t = t1 - t0
        mem_usage = max(mem_usage_after) - min(mem_usage_before)

        return Stats(
            time_seconds=t,
            memory_usage_MiB=mem_usage,
            filesize=os.path.getsize(output_filename),
        )

    @staticmethod
    @profile_me
    def query_single_summary_key_for_one_realization(
        ds_file: str, response_key: str, realization: int
    ) -> Optional[Union[pd.DataFrame, Stats]]:
        if os.path.exists(ds_file):
            ds = pl.read_ipc(ds_file)
            pddf = (
                ds.filter((pl.col("realization") == realization))
                .filter((pl.col("response_key") == response_key))
                .select(["time", "value"])
            )

            return pddf
