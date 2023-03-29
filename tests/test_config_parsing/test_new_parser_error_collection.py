from textwrap import dedent

import pytest

from ert._c_wrappers.config import ConfigValidationError
from ert._c_wrappers.config.config_parser import ErrorInfo
from ert._c_wrappers.enkf import ErtConfig


@pytest.mark.usefixtures("use_tmpdir")
def test_info_queue_content_negative_value_invalid():
    test_config_file_base = "test"
    test_config_file_name = f"{test_config_file_base}.ert"
    test_config_contents = dedent(
        """
NUM_REALIZATIONS  1
DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
ENSPATH <STORAGE>/ensemble
QUEUE_SYSTEM LOCAL
QUEUE_OPTION LOCAL MAX_RUNNING -4
"""
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)
    with pytest.raises(
        expected_exception=ConfigValidationError,
        match="QUEUE_OPTION LOCAL MAX_RUNNING is negative",
    ):
        collected_errors = []
        ErtConfig.from_file(
            test_config_file_name,
            use_new_parser=True,
            collected_errors=collected_errors,
        )

        error: ErrorInfo = collected_errors[0]
        assert error.filename == test_config_file_name
        assert error.line == 7
        assert error.column == 32
        assert error.end_column == 34
        ConfigValidationError.raise_from_collected(collected_errors)
