from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


@dataclass
class Stats:
    memory_usage_MiB: float
    time_seconds: float
    filesize: Optional[float] = None

    def to_dict(self) -> Dict[str, float]:
        return {
            "memory_usage_MiB": self.memory_usage_MiB,
            "time_seconds": self.time_seconds,
            "filesize": self.filesize,
        }

    @classmethod
    def from_dict(cls, the_dict: dict):
        return cls(
            memory_usage_MiB=the_dict["memory_usage_MiB"],
            time_seconds=the_dict["time_seconds"],
            filesize=the_dict["filesize"],
        )


class DatalibHooks(ABC):
    @staticmethod
    @abstractmethod
    def from_dataframe_to_file(
        df: pd.DataFrame, output_filename: str
    ) -> Optional[Stats]:
        """
        Transforms the data from a pandas dataframe into a
        file of
        """

    @staticmethod
    @abstractmethod
    def from_file_to_dataframe(filename: str) -> Optional[Tuple[pd.DataFrame, Stats]]:
        """
        Transforms the data from a pandas dataframe into a
        file of
        """

    @staticmethod
    @abstractmethod
    def combine_one_ds_per_response_key(
        response_keys: List[str], num_reals: int
    ) -> Optional[Stats]:
        """
        Combine datasets with one dataset per response key
        """

    @staticmethod
    @abstractmethod
    def combine_one_ds_many_response_keys(
        ds_file: str, num_reals: int
    ) -> Optional[Stats]:
        """
        Combine datasets with multiple response keys
        """

    @staticmethod
    @abstractmethod
    def query_single_realization_for_all_existing_responses(
        ds_file: str, realization: int
    ) -> Optional[Tuple[Set[str], Stats]]:
        """
        Query for a single realization with all responses
        """

    @staticmethod
    @abstractmethod
    def query_all_realizations_with_all_responses(
        ds_file: str,
    ) -> Optional[Tuple[Set[int], Stats]]:
        """
        Query to find all realizations with all responses
        """

    @staticmethod
    @abstractmethod
    def query_single_summary_key_for_one_realization(
        ds_file: str, response_key: str, realization: int
    ) -> Optional[Tuple[Any, Stats]]:
        """
        Get list of (timestep, value) series for a single summary key for one
        realization
        """

    @staticmethod
    @abstractmethod
    def query_single_summary_key_for_all_realizations(
        ds_file: str, response_key: str
    ) -> Optional[Tuple[Any, Stats]]:
        """
        Get list of (realization, timestep, value) series for a single summary key for one
        realization
        """

    @staticmethod
    @abstractmethod
    def join_observations_and_responses(
        responses_ds_file: str, observations_file: str, primary_key: List[str]
    ) -> Optional[Tuple[Any, Stats]]:
        """
        Joins a combined dataset with a (combined) observations dataset
        """
