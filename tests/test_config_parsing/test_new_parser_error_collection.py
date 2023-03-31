from textwrap import dedent

import pytest

from ert._c_wrappers.config.config_parser import ErrorInfo
from ert._c_wrappers.enkf import ErtConfig

test_config_file_base = "test"
test_config_file_name = f"{test_config_file_base}.ert"


def assert_error_from_config_with(
    contents: str,
    expected_line: int,
    expected_column: int,
    expected_end_column: int,
    file_name: str = "test.ert",
    other_files: dict = None,
):
    with open(file_name, "w", encoding="utf-8") as fh:
        fh.write(contents)

    if other_files is not None:
        for other_file_name, content in other_files.items():
            with open(other_file_name, mode="w", encoding="utf-8") as fh:
                fh.writelines(content)

    collected_errors = []
    ErtConfig.from_file(
        file_name, use_new_parser=True, collected_errors=collected_errors
    )

    for error in collected_errors:
        assert error.filename == file_name

    error_locations = [(x.line, x.column, x.end_column) for x in collected_errors]
    expected_error_loc = (expected_line, expected_column, expected_end_column)

    assert expected_error_loc in error_locations, (
        f"Expected error location {expected_error_loc} to be found in list of"
        f"error locations: {error_locations}"
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_queue_content_negative_value_invalid():
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS  1
DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
ENSPATH <STORAGE>/ensemble
QUEUE_SYSTEM LOCAL
QUEUE_OPTION LOCAL MAX_RUNNING -4
        """,
        expected_line=7,
        expected_column=32,
        expected_end_column=34,
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_summary_given_without_eclbase_gives_error(tmp_path):
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS 1
SUMMARY summary""",
        expected_line=3,
        expected_column=1,
        expected_end_column=8,
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_gen_kw_with_incorrect_format(tmp_path):
    assert_error_from_config_with(
        contents="""
JOBNAME my_name%d
NUM_REALIZATIONS 1
GEN_KW KW_NAME template.txt kw.txt priors.txt INIT_FILES:custom_param0
""",
        expected_line=4,
        expected_column=47,
        expected_end_column=71,
        other_files={
            "template.txt": "MY_KEYWORD <MY_KEYWORD>",
            "priors.txt": "MY_KEYWORD NORMAL 0 1",
        },
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_gen_kw_forward_init(tmp_path):
    assert_error_from_config_with(
        contents="""
JOBNAME my_name%d
NUM_REALIZATIONS 1
GEN_KW KW_NAME template.txt kw.txt priors.txt FORWARD_INIT:True INIT_FILES:custom_param
    """,
        expected_line=4,
        expected_column=47,
        expected_end_column=64,
        other_files={
            "template.txt": "MY_KEYWORD <MY_KEYWORD>",
            "priors.txt": "MY_KEYWORD NORMAL 0 1",
        },
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_missing_forward_model_job(tmp_path):
    assert_error_from_config_with(
        contents="""
JOBNAME my_name%d
NUM_REALIZATIONS 1
FORWARD_MODEL ECLIPSE9001
""",
        expected_line=4,
        expected_column=15,
        expected_end_column=26,
        other_files={
            "template.txt": "MY_KEYWORD <MY_KEYWORD>",
            "priors.txt": "MY_KEYWORD NORMAL 0 1",
        },
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_positional_forward_model_args_gives_error():
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS  1
FORWARD_MODEL RMS <IENS>
""",
        expected_line=3,
        expected_column=15,
        expected_end_column=25,
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_missing_simulation_job_gives_error():
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS  1
SIMULATION_JOB this-is-not-the-job-you-are-looking-for hello
        """,
        expected_line=3,
        expected_column=16,
        expected_end_column=55,
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_info_unknown_hooked_job_gives_error():
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS  1
HOOK_WORKFLOW NO_SUCH_JOB PRE_SIMULATION
""",
        expected_line=3,
        expected_column=15,
        expected_end_column=26,
    )


@pytest.mark.usefixtures("use_tmpdir")
def test_that_non_exising_job_directory_gives_error():
    assert_error_from_config_with(
        contents="""
NUM_REALIZATIONS  1
DEFINE <STORAGE> storage/<CONFIG_FILE_BASE>-<DATE>
RUNPATH <STORAGE>/runpath/realization-<IENS>/iter-<ITER>
ENSPATH <STORAGE>/ensemble
INSTALL_JOB_DIRECTORY does_not_exist
""",
        expected_line=6,
        expected_column=23,
        expected_end_column=37,
    )
