import dataclasses
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

import xarray as xr

from ert.config.commons import Refcase
from ert.config.parameter_config import CustomDict
from ert.config.parsing import ContextList, ContextValue
from ert.config.responses.observation_vector import ObsVector

from ..parsing.config_errors import ConfigValidationError


@dataclasses.dataclass
class ObsArgs:
    obs_name: str
    refcase: Optional[Refcase]
    values: Any
    std_cutoff: Any
    history: Optional[Any]
    obs_time_list: Any
    config_for_response: Optional["ResponseConfig"] = None


@dataclasses.dataclass
class ObsArgsNew:
    """
    Temporarily named like this, shall replace ObsArgs after porting
    GenDataConfig and SummaryConfig
    """

    refcase: Optional[Refcase]
    std_cutoff: Any
    history: Optional[Any]
    obs_time_list: Any


_PATTERN = re.compile(r"(<[^>]+>)=([^,]+?)(?=,|\)$)")


class ObservationConfig:
    """Holds info for parsing an observation towards certain responses,
     or response types
    Attributes:
        src: The source file containing the observations data
        obs_name: The name of the observation, may be None and embedded within the src
            file.
        keyword_token: First word of line in the ert config where the user
            specified this observation
        line_from_ert_config: Words following the keyword_token in the ert config,
            on the line where the user specified this observation
        response_type: Type of the observed response
        response_name: Name of the observed response. May be None which implies
            the name exists within the src file.
    """

    def __init__(
        self,
        line_from_ert_config: ContextList[ContextValue],
        owner_response_type: str,
    ):
        """
            Initializes an observation config from the arguments passed by the user
            through the ert config.
        Args:
            line_from_ert_config: List of arguments passed in the ert config
            owner_response_type: Response type that contains the keyword,
                i.e., if the keyword is "X", then that response's
                .ert_config_observation_keyword() method returns "X"
                 that identifies this observation in the ert config.

        """
        kwargs = ResponseConfigWithLifecycleHooks.parse_kwargs_from_config_list(
            line_from_ert_config[0]
        )

        response_name = kwargs.get("<RESPONSE_NAME>")
        response_type = kwargs.get("<RESPONSE_TYPE>", owner_response_type)
        obs_name = kwargs.get("<OBS_NAME>", f"obs{line_from_ert_config}")

        # Expect <SRC> always
        if "<SRC>" not in kwargs:
            raise ConfigValidationError(
                "Observation must have <SRC> keyword argument to specify the"
                "source of the observation."
            )

        self.src = kwargs["<SRC>"]
        self.obs_name = obs_name
        self.keyword_token = line_from_ert_config.keyword_token
        self.line_from_ert_config = line_from_ert_config[0]
        self.response_type = response_type
        self.response_name = response_name


class ResponseConfigWithLifecycleHooks(ABC):
    def __init__(
        self,
        line_from_ert_config: List[ContextValue],
    ):
        self.line_from_ert_config = line_from_ert_config

    @property
    def src(self):
        if len(self.line_from_ert_config) == 1:
            kwargs = self.parse_kwargs_from_config_list(self.line_from_ert_config[0])
            if "<SRC>" not in kwargs:
                raise ConfigValidationError(
                    f"Response of type {self.response_type()} must "
                    f"have <SRC> keyword argument to specify the name of the file"
                    f"it should be read from."
                )

            return kwargs["<SRC>"]

        raise ConfigValidationError(
            f"Response {self.name} expected args in format (<K>=V,...)"
        )

    @property
    def name(self):
        if len(self.line_from_ert_config) == 1:
            kwargs = self.parse_kwargs_from_config_list(self.line_from_ert_config[0])
            return kwargs.get(
                "<NAME>",
                f"response{self.line_from_ert_config[0]}",
            )

    @classmethod
    @abstractmethod
    def from_config_list(
        cls, config_list: List[ContextList[ContextValue]]
    ) -> Union[
        "ResponseConfigWithLifecycleHooks", List["ResponseConfigWithLifecycleHooks"]
    ]:
        """
        Takes the list of entries for the given keyword and returns
        either a list of configs (gen_data will do this), or a single
        config that results from multiple line entries (summary does this).
        It is up to the implementation of the response whether it should
        turn N lines from the ert config file into N configs, or into a single config
        with all the information.
        """

    @classmethod
    def parse_kwargs_from_config_list(cls, config_list: ContextValue) -> Dict[str, str]:
        return {m[1]: m[2] for m in _PATTERN.finditer(config_list)}

    @classmethod
    @abstractmethod
    def response_type(cls) -> str:
        """Denotes the name for the entire response type. Not to be confused
        with the name of the specific response. This would for example be
        GEN_DATA, whilst WOPR:OP1 would be the name of an instance of this class
        """

    @classmethod
    @abstractmethod
    def ert_config_response_keyword(cls) -> Union[List[str], str]:
        """Denotes the keyword to be used in the ert config to give responses
            of the implemented type. For example CSV_RESPONSE.
        :return: The ert config keyword for specifying responses for this type.
        """

    @classmethod
    @abstractmethod
    def ert_config_observation_keyword(cls) -> Union[List[str], str]:
        """Denotes the keyword to be used in the ert config to give observations
            on responses of this type. For example CSV_OBSERVATION.
        :return: The ert config keyword for specifying observations
                 on this response type.
        """

    @abstractmethod
    def parse_response_from_config(
        self, response_kwargs_from_ert_config: Dict[str, str]
    ) -> None:
        """
        Parses the response given the keyword arguments specified in the config.
        For example, if the ert config kwargs is
        (<A>=2,<B>=heh), then response_kwargs_from_ert_config will be
        {"<A>": "2", "<B>": "heh"}
        """

    @staticmethod
    @abstractmethod
    def parse_observation_from_legacy_obsconfig(
        args: ObsArgs,
    ) -> Dict[str, ObsVector]: ...

    @abstractmethod
    def parse_observation_from_config(
        self, obs_config: ObservationConfig, obs_args: ObsArgsNew
    ) -> xr.Dataset:
        """
        Parses the observation given the keyword arguments specified
        for the observation entry in the config. For example, if line 5 in the
        ert config is CSV_OBSERVATION(<SRC>="22.txt"), the kwargs will be
        {"<SRC>": "22.txt", "OBS_NAME_DEFAULT": "Observation@line:5"}
        The "OBS_NAME_DEFAULT" is only meant to be used if the <SRC> entry itself
        does not contain a list of observation names.
        """
        pass

    @abstractmethod
    def parse_response_from_runpath(self, run_path: str) -> xr.Dataset:
        pass


@dataclasses.dataclass
class ResponseConfig(ABC):
    name: str

    @staticmethod
    @abstractmethod
    def parse_observation_from_legacy_obsconfig(
        args: ObsArgs,
    ) -> Dict[str, ObsVector]: ...

    @abstractmethod
    def read_from_file(self, run_path: str, iens: int) -> xr.Dataset: ...

    def to_dict(self) -> Dict[str, Any]:
        data = dataclasses.asdict(self, dict_factory=CustomDict)
        data["_ert_kind"] = self.__class__.__name__
        return data
