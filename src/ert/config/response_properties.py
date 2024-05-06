from ert.enum_shim import StrEnum


class ResponseTypes(StrEnum):
    """
    Represents response types internal to ert.
    """

    GEN_DATA = "GEN_DATA"
    SUMMARY = "SUMMARY"
