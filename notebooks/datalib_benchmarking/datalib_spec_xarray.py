import json
import os.path
import time
from typing import List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from memory_profiler import memory_usage
from notebooks.datalib_benchmarking.datalib_spec import DatalibHooks, Stats
from notebooks.datalib_benchmarking.utils import (
    p_out,
    p_real,
    profile_me,
    profile_me_stats_only,
)


class XRHooks(DatalibHooks):
    @staticmethod
    @profile_me
    def from_file_to_dataframe(filename: str) -> Tuple[Optional[pd.DataFrame], Stats]:
        if not os.path.exists(filename):
            return None

        return xr.open_dataset(filename).to_dataframe().dropna().reset_index()

    @staticmethod
    @profile_me
    def query_single_summary_key_for_all_realizations(
        ds_file: str, response_key: str
    ) -> Tuple[Optional[pd.DataFrame], Stats]:
        if os.path.exists(ds_file):
            ds = xr.open_dataset(ds_file)

            result = ds.sel(response_key=response_key, drop=True)

            # result_df = result.to_dataframe().reset_index()

            return result

    @staticmethod
    @profile_me
    def join_observations_and_responses(
        responses_ds_file: str, observations_file: str, primary_key: List[str]
    ) -> Tuple[Optional[pd.DataFrame], Stats]:
        numerical_data = []
        index_data = []

        obs_datasets = xr.load_dataset(observations_file)
        obs_keys_to_check = set(obs_datasets["obs_key"].data)

        responses_ds = xr.open_dataset(responses_ds_file)
        index = primary_key

        for obs_key in obs_keys_to_check:
            obs_ds = obs_datasets.sel(obs_key=obs_key, drop=True)

            obs_ds = obs_ds.dropna("response_key", subset=["observation"], how="all")
            for k in index:
                obs_ds = obs_ds.dropna(dim=k, how="all")

            response_keys_to_check = obs_ds["response_key"].data

            for response_key in response_keys_to_check:
                observations_for_response = obs_ds.sel(
                    response_key=response_key, drop=True
                )

                responses_matching_obs = responses_ds.sel(
                    response_key=response_key, drop=True
                )

                combined = observations_for_response.merge(
                    responses_matching_obs, join="left"
                )

                response_vals_per_real = combined["value"].stack(key=index).values.T

                key_index_1d = np.array(
                    [
                        (
                            x.strftime("%Y-%m-%d")
                            if isinstance(x, pd.Timestamp)
                            else json.dumps(x)
                        )
                        for x in combined[index].coords.to_index()
                    ]
                )
                obs_vals_1d = combined["observation"].data
                std_vals_1d = combined["error"].data

                num_obs = len(obs_vals_1d)
                obs_keys_1d = np.array([obs_key] * num_obs)

                if (
                    len(key_index_1d) != num_obs
                    or response_vals_per_real.shape[0] != num_obs
                    or len(std_vals_1d) != num_obs
                ):
                    raise IndexError(
                        "Axis 0 misalignment, expected axis 0 length to "
                        f"correspond to observation names {num_obs}. Got:\n"
                        f"len(response_vals_per_real)={len(response_vals_per_real)}\n"
                        f"len(obs_keys_1d)={len(obs_keys_1d)}\n"
                        f"len(std_vals_1d)={len(std_vals_1d)}"
                    )

                # if response_vals_per_real.shape[1] != len(reals_with_responses_mask):
                #    raise IndexError(
                #        "Axis 1 misalignment, expected axis 1 of"
                #        f" response_vals_per_real to be the same as number of realizations"
                #        f" with responses ({len(reals_with_responses_mask)}),"
                #        f"but got response_vals_per_real.shape[1]"
                #        f"={response_vals_per_real.shape[1]}"
                #    )

                _index_data = np.concatenate(
                    [
                        obs_keys_1d.reshape(-1, 1),
                        key_index_1d.reshape(-1, 1),
                    ],
                    axis=1,
                )
                index_data.append(_index_data)
                _numerical_data = np.concatenate(
                    [
                        obs_vals_1d.reshape(-1, 1),
                        std_vals_1d.reshape(-1, 1),
                        response_vals_per_real,
                    ],
                    axis=1,
                )
                numerical_data.append(_numerical_data)

        if not index_data:
            msg = (
                "No observation: "
                + (
                    ", ".join(obs_keys_to_check)
                    if obs_keys_to_check is not None
                    else "*"
                )
                + " in ensemble"
            )
            raise KeyError(msg)

        index_df = pd.DataFrame(
            np.concatenate(index_data), columns=["response_key", "key_index"]
        )
        numerical_df = pd.DataFrame(
            np.concatenate(numerical_data),
            columns=["observation", "error"]
            + list(range(response_vals_per_real.shape[1])),
        )
        result_df = pd.concat([index_df, numerical_df], axis=1)
        result_df.sort_values(by=["response_key", "key_index"], inplace=True)

        return result_df

    @staticmethod
    @profile_me
    def query_single_summary_key_for_one_realization(
        ds_file: str, response_key: str, realization: int
    ) -> Tuple[Optional[pd.DataFrame], Stats]:
        if os.path.exists(ds_file):
            ds = xr.open_dataset(ds_file)

            result = ds.sel(realization=realization, drop=True).sel(
                response_key=response_key, drop=True
            )

            result_df = result.to_dataframe().reset_index()

            return result_df

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_per_response_key(
        response_keys: List[str], num_reals: int
    ) -> Optional[Stats]:
        all_ds = []
        for i in range(num_reals):
            ds_for_names = []
            for key in response_keys:
                fp = f"{p_real(i)}/{key}.xarray"
                if os.path.exists(fp):
                    ds_for_names.append(
                        xr.open_dataset(fp).expand_dims(response_key=[key])
                    )

            if len(ds_for_names) > 0:
                ds_for_realization = xr.concat(
                    ds_for_names, dim="response_key"
                ).expand_dims(realization=[i])
                all_ds.append(ds_for_realization)

        combined = xr.concat(all_ds, dim="realization")
        combined.to_netcdf(p_out / "gen_data_combined.xarray")

    @staticmethod
    @profile_me_stats_only
    def combine_one_ds_many_response_keys(
        ds_file: str, num_reals: int
    ) -> Optional[Stats]:
        all_ds = []
        for i in range(num_reals):
            fp = f"{p_real(i)}/{ds_file}"
            if os.path.exists(fp):
                ds_for_realization = xr.open_dataset(fp).expand_dims(realization=[i])
                all_ds.append(ds_for_realization)

        combined = xr.concat(all_ds, dim="realization")
        combined.to_netcdf(p_out / "summary_combined.xarray")

    @staticmethod
    @profile_me
    def query_single_realization_for_all_existing_responses(
        ds_file: str, realization: int
    ) -> Optional[Tuple[Set[str], Stats]]:
        # Insert logic here
        ds = xr.open_dataset(ds_file)
        # 1. Filter for realization
        ds = ds.sel(realization=realization, drop=True)
        ds = ds.dropna(dim="time", how="any")
        keys = set(ds["response_key"].data)

        return keys

    @staticmethod
    @profile_me
    def query_all_realizations_with_all_responses(
        ds_file: str,
    ) -> Optional[Tuple[Set[int], Stats]]:
        # Insert logic here
        ds = xr.open_dataset(ds_file)
        # 1. Filter for realization
        reals_with_all_responses = (
            ds.groupby("realization")
            .count("time")
            .count("response_key")["realization"]
            .data.tolist()
        )

        return reals_with_all_responses

    @staticmethod
    def from_dataframe_to_file(df: pd.DataFrame, output_filename: str) -> Stats:
        xarray_dataset = df.to_xarray()
        t0 = time.time()
        mem_usage_before = memory_usage(-1, interval=0.01, timeout=0.1)
        xarray_dataset.to_netcdf(output_filename)
        t1 = time.time()
        mem_usage_after = memory_usage(-1, interval=0.01, timeout=0.1)
        t = t1 - t0
        mem_usage = max(mem_usage_after) - min(mem_usage_before)

        return Stats(
            memory_usage_MiB=mem_usage,
            time_seconds=t,
            filesize=os.path.getsize(output_filename),
        )
