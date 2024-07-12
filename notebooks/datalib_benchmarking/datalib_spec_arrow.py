import os.path
import time
from typing import List, Optional, Set, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
from memory_profiler import memory_usage
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks, Stats
from notebooks.datalib_benchmarking.utils import (
    p_out,
    p_real,
    profile_me,
    profile_me_stats_only,
)


class ArrowHooks(DatalibHooks):
    @staticmethod
    @profile_me
    def from_file_to_dataframe(filename: str) -> Optional[Tuple[pd.DataFrame, Stats]]:
        ds = pads.dataset(str(filename), format="ipc")
        return ds.to_table().to_pandas().reset_index()

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_per_response_key(
        response_keys: List[str], num_reals: int
    ) -> Optional[Stats]:
        all_ds = []

        for i in range(num_reals):
            for key in response_keys:
                fp = f"{p_real(i)}/{key}.arrow"
                if os.path.exists(fp):
                    ds = pa.RecordBatchFileReader(f"{p_real(i)}/{key}.arrow").read_all()
                    realization_column = pa.array([i] * len(ds))
                    name_column = pa.array([key] * len(ds))
                    ds = ds.append_column("index", realization_column)
                    ds = ds.append_column("response_key", name_column)
                    all_ds.append(ds)

        concatd = pa.concat_tables(all_ds)
        with pa.OSFile(str(p_out / "gen_data_combined.arrow"), "wb") as f:
            writer = pa.RecordBatchFileWriter(f, concatd.schema)
            writer.write_table(concatd)
            writer.close()

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_many_response_keys(
        ds_file: str, num_reals: int
    ) -> Optional[Stats]:
        all_ds = []

        for i in range(num_reals):
            fp = f"{p_real(i)}/{ds_file}"
            if os.path.exists(fp):
                ds = pa.RecordBatchFileReader(fp).read_all()
                realization_column = pa.array([i] * len(ds))
                ds = ds.append_column("realization", realization_column)
                all_ds.append(ds)

        concatd = pa.concat_tables(all_ds)
        with pa.OSFile(str(p_out / "summary_combined.arrow"), "wb") as f:
            writer = pa.RecordBatchFileWriter(f, concatd.schema)
            writer.write_table(concatd)
            writer.close()

    @staticmethod
    @profile_me
    def query_single_realization_for_all_existing_responses(
        ds_file: str, realization: int
    ) -> Optional[Tuple[Set[str], Stats]]:
        ds = pads.dataset(str(ds_file), format="ipc")
        table = ds.to_table(
            columns=["response_key"], filter=(pc.field("realization") == realization)
        )

        uniq = pc.unique(table["response_key"]).to_pylist()

        return set(uniq)

    @staticmethod
    @profile_me
    def query_all_realizations_with_all_responses(
        ds_file: str,
    ) -> Optional[Tuple[Set[int], Stats]]:
        ds = pads.dataset(str(ds_file), format="ipc")
        table = ds.to_table(columns=["realization", "response_key"])
        response_key_count_histogram = table.group_by("realization").aggregate(
            [("response_key", "count_distinct")]
        )
        mask = pc.greater_equal(
            response_key_count_histogram["response_key_count_distinct"], 400
        )
        filtered = response_key_count_histogram.filter(mask)

        reals_with_all_responses = filtered["realization"].to_pylist()
        return reals_with_all_responses

    @staticmethod
    def from_dataframe_to_file(df: pd.DataFrame, output_filename: str) -> Stats:
        # Convert pandas DataFrame to Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        t0 = time.time()
        mem_usage_before = memory_usage(-1, interval=0.01, timeout=0.1)
        with pa.OSFile(str(output_filename), "wb") as f:
            writer = pa.RecordBatchFileWriter(f, arrow_table.schema)
            writer.write_table(arrow_table)
            writer.close()
        t1 = time.time()
        mem_usage_after = memory_usage(-1, interval=0.01, timeout=0.1)
        t = t1 - t0
        mem_usage = max(mem_usage_after) - min(mem_usage_before)

        filesize = os.path.getsize(output_filename)
        return Stats(time_seconds=t, memory_usage_MiB=mem_usage, filesize=filesize)

    @staticmethod
    @profile_me
    def query_single_summary_key_for_all_realizations(
        ds_file: str, response_key: str
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        ds = pads.dataset(str(ds_file), format="ipc")

        table = ds.to_table(
            columns=["realization", "response_key", "time", "value"],
            filter=(pc.field("response_key") == response_key),
        )

        return table

    @staticmethod
    @profile_me
    def query_single_summary_key_for_one_realization(
        ds_file: str, response_key: str, realization: int
    ) -> Optional[Tuple[List[Tuple[float, float]], Stats]]:
        ds = pads.dataset(str(ds_file), format="ipc")

        table = ds.to_table(
            columns=["realization", "response_key", "time", "value"],
            filter=(
                (pc.field("response_key") == response_key)
                & (pc.field("realization") == realization)
            ),
        )

        return table

    @staticmethod
    @profile_me
    def join_observations_and_responses(
        responses_ds_file: str, observations_file: str, primary_key: List[str]
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        # Clunky, doesn't support joining on string keys
        # Will support dict-encoding it then joining on that
        # (would expect that to already be there but seems not)
        pass
