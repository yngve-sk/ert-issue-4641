from textwrap import dedent

import pytest

from ert._c_wrappers.config.config_parser import ErrorInfo
from ert._c_wrappers.enkf import ErtConfig

test_config_file_base = "test"
test_config_file_name = f"{test_config_file_base}.ert"


@pytest.mark.usefixtures("use_tmpdir")
def test_info_queue_content_negative_value_invalid():
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


def test_info_summary_given_without_eclbase_gives_error(tmp_path):
    (tmp_path / "config.ert").write_text(
        """NUM_REALIZATIONS 1
SUMMARY summary"""
    )

    collected_errors = []
    ErtConfig.from_file(
        str(tmp_path / "config.ert"),
        collected_errors=collected_errors,
        use_new_parser=True,
    )

    error: ErrorInfo = collected_errors[0]

    assert error.line == 2
    assert error.column == 1
    assert error.end_column == 8


@pytest.mark.usefixtures("use_tmpdir")
def test_info_gen_kw_with_incorrect_format(tmp_path):
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(
            """
JOBNAME my_name%d
NUM_REALIZATIONS 1
GEN_KW KW_NAME template.txt kw.txt prior.txt INIT_FILES:custom_param0
"""
        )

    with open("template.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD <MY_KEYWORD>")

    with open("prior.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD NORMAL 0 1")

    collected_errors = []
    ErtConfig.from_file(
        test_config_file_name,
        use_new_parser=True,
        collected_errors=collected_errors,
    )

    error: ErrorInfo = collected_errors[0]
    assert error.filename == test_config_file_name
    assert error.line == 4
    assert error.column == 46
    assert error.end_column == 70


@pytest.mark.usefixtures("use_tmpdir")
def test_info_gen_kw_forward_init(tmp_path):
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(
            """
JOBNAME my_name%d
NUM_REALIZATIONS 1
GEN_KW KW_NAME template.txt kw.txt priors.txt FORWARD_INIT:True INIT_FILES:custom_param
"""
        )

    with open("template.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD <MY_KEYWORD>")

    with open("priors.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD NORMAL 0 1")

    collected_errors = []
    ErtConfig.from_file(
        test_config_file_name,
        use_new_parser=True,
        collected_errors=collected_errors,
    )

    error: ErrorInfo = collected_errors[0]
    assert error.filename == test_config_file_name
    assert error.line == 4
    assert error.column == 47
    assert error.end_column == 64


@pytest.mark.usefixtures("use_tmpdir")
def test_info_missing_forward_model_job(tmp_path):
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(
            """
JOBNAME my_name%d
NUM_REALIZATIONS 1
FORWARD_MODEL ECLIPSE9001
"""
        )

    with open("template.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD <MY_KEYWORD>")

    with open("priors.txt", mode="w", encoding="utf-8") as fh:
        fh.writelines("MY_KEYWORD NORMAL 0 1")

    collected_errors = []
    ErtConfig.from_file(
        test_config_file_name,
        use_new_parser=True,
        collected_errors=collected_errors,
    )

    error: ErrorInfo = collected_errors[0]
    assert error.filename == test_config_file_name
    assert error.line == 4
    assert error.column == 15
    assert error.end_column == 26


@pytest.mark.usefixtures("use_tmpdir")
def test_info_positional_forward_model_args_gives_config_validation_error():
    test_config_contents = dedent(
        """
NUM_REALIZATIONS  1
FORWARD_MODEL RMS <IENS>
        """
    )
    with open(test_config_file_name, "w", encoding="utf-8") as fh:
        fh.write(test_config_contents)

    collected_errors = []
    ErtConfig.from_file(
        test_config_file_name, use_new_parser=True, collected_errors=collected_errors
    )

    error: ErrorInfo = collected_errors[0]
    assert error.filename == test_config_file_name
    assert error.column == 15
    assert error.end_column == 25
