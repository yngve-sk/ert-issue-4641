from typing import List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pyarrow.dataset as pads
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks, Stats
from notebooks.datalib_benchmarking.utils import profile_me


class DuckdbHooks(DatalibHooks):
    @staticmethod
    def from_file_to_dataframe(filename: str) -> Optional[Tuple[pd.DataFrame, Stats]]:
        return None

    @staticmethod
    @profile_me
    def query_single_summary_key_for_all_realizations(
        ds_file: str, response_key: str
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        con = duckdb.connect()
        ipc_file_path = str(ds_file).replace(".duckdb", ".arrow")

        ds = pads.dataset(ipc_file_path, format="ipc")
        con.register("ds", ds)
        query = f"""
                  SELECT realization, time, value FROM ds
                  WHERE response_key = '{response_key}'
             """
        result = con.execute(query)

        return result

    @staticmethod
    @profile_me
    def join_observations_and_responses(
        responses_ds_file: str, observations_file: str, primary_key: List[str]
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        ## Read the Arrow IPC (Feather) files
        # responses_arrow = pads.dataset(
        #    responses_ds_file.replace(".duckdb", ".arrow"), format="ipc"
        # )
        # obs_arrow = pads.dataset(
        #    observations_file.replace(".duckdb", ".arrow"), format="ipc"
        # )
        #
        ## Connect to DuckDB and register the Arrow Tables
        # duckdb_conn = duckdb.connect()
        # duckdb_conn.register("responses_arrow", responses_arrow)
        # duckdb_conn.register("obs_arrow", obs_arrow)
        #
        ## Construct the SQL query for pivoting and joining
        # primary_key_str = ", ".join(primary_key)
        #
        # sql_query = f"""
        #    PIVOT responses_ds ON realization USING value
        # """
        #
        ## Execute the query and fetch the result as an Arrow Table
        # result_arrow = duckdb_conn.execute(sql_query).arrow()

        return None

    @staticmethod
    def combine_one_ds_per_response_key(
        response_keys: List[str], num_reals: int
    ) -> Optional[Stats]:
        pass

    @staticmethod
    def combine_one_ds_many_response_keys(
        ds_file: str, num_reals: int
    ) -> Optional[Stats]:
        pass

    @staticmethod
    @profile_me
    def query_single_realization_for_all_existing_responses(
        ds_file: str, realization: int
    ) -> Optional[Tuple[Set[str], Stats]]:
        # Insert logic here
        # 1. Filter out for realization
        # 2. Find set of keys in remaining ds
        # 3. Check that this set of keys corresponds to the set of keys from all ds

        con = duckdb.connect()
        ipc_file_path = str(ds_file).replace(".duckdb", ".arrow")

        ds = pads.dataset(ipc_file_path, format="ipc")
        con.register("ds", ds)
        query = f"""
                           SELECT DISTINCT response_key
                           FROM ds WHERE realization = {realization}
                       """
        result = con.execute(query).fetchnumpy()["response_key"].tolist()

        return set(result)

    @staticmethod
    @profile_me
    def query_all_realizations_with_all_responses(
        ds_file: str,
    ) -> Optional[Tuple[Set[int], Stats]]:
        # Insert logic here
        # 1. Filter out for realization
        # 2. Find set of keys in remaining ds
        # 3. Check that this set of keys corresponds to the set of keys from all ds

        con = duckdb.connect()
        ipc_file_path = str(ds_file).replace(".duckdb", ".arrow")

        ds = pads.dataset(ipc_file_path, format="ipc")
        con.register("ds", ds)
        query = """
                WITH response_key_count AS (
                    SELECT realization, COUNT(DISTINCT response_key) AS response_key_count
                    FROM ds
                    GROUP BY realization
                )
                SELECT realization
                FROM response_key_count
                WHERE response_key_count.response_key_count = (
                    SELECT COUNT(DISTINCT response_key)
                    FROM ds
                )
            """
        result = con.execute(query).fetchnumpy()["realization"]

        return result

    @staticmethod
    def from_dataframe_to_file(
        df: pd.DataFrame, output_filename: str
    ) -> Optional[Stats]:
        # TODO if we want use duckdb format to store data(?)
        # (seems to be somewhat widely used)
        return None

    @staticmethod
    @profile_me
    def query_single_summary_key_for_one_realization(
        ds_file: str, response_key: str, realization: int
    ) -> Optional[Tuple[pd.DataFrame, Stats]]:
        con = duckdb.connect()
        ipc_file_path = str(ds_file).replace(".duckdb", ".arrow")

        ds = pads.dataset(ipc_file_path, format="ipc")
        con.register("ds", ds)
        query = f"""
            SELECT time, value FROM ds
            WHERE realization = '{realization}' AND response_key = '{response_key}'
       """
        result = con.execute(query)

        return result
