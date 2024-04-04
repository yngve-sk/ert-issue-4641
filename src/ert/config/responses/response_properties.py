from enum import StrEnum, auto


class ResponseDataInitialLayout(StrEnum):
    """
    Represents how data from a forward model run is organized initially within
    each realization folder.
    """

    ONE_FILE_PER_NAME = auto()  # ex: gen data files
    ONE_FILE_WITH_ALL_NAMES = auto()  # ex: summary files


class ResponseTypes(StrEnum):
    """
    Represents response types internal to ert.
    """

    GEN_DATA = auto()
    SUMMARY = auto()
