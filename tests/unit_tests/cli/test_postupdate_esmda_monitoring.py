from argparse import ArgumentParser

import pytest
from ert.__main__ import ert_parser
from ert.cli.main import run_cli


@pytest.mark.usefixtures("copy_snake_oil_case")
def test_intermediate_monitoring_of_snake_oil(prior_ensemble):
    with open("snake_oil.ert", "a", encoding="utf-8") as ert_file:
        ert_file.write(
            """
        LOAD_WORKFLOW_JOB ASSERT_LAST_STEP_REFS wfj_assertlast
        LOAD_WORKFLOW ASSERT_LAST_STEP_REFS_WORKFLOW wf_assertlast
        HOOK_WORKFLOW wf_assertlast POST_UPDATE
        """
        )

    with open("ASSERT_LAST_STEP_REFS", "w+", encoding="utf-8") as wfj_file:
        wfj_file.write(
            """INTERNAL True
                SCRIPT assert_last_ref_step.py
        """
        )

    with open("ASSERT_LAST_STEP_REFS_WORKFLOW", "w+", encoding="utf-8") as wf_file:
        wf_file.write("wfj_assertlast <ITER> <IENS>")

    with open("assert_last_ref_step.py", "w+", encoding="utf-8") as wf_file:
        wf_file.write(
            """
from ert import ErtScript
import json
import pathlib


class AssertReferenceIntegrityForPreviousIteration(ErtScript):
    def __init__(self, ert, storage, ensemble=None):
        super().__init__(ert, storage, ensemble=ensemble)
        
        next_iteration = ensemble.iteration
        iter = next_iteration - 1
        
        reference_folder = os.path.join(os.getcwd(), "expected_parameters")
        runpath_folder = os.path.join(os.getcwd(), "storage", "snake_oil", "runpath")
        
        def get_params_file(folder, iens):
            file_path = os.path.join(
                folder,
                f"realization-{iens}",
                f"iter-{iter}", "parameters.json"
            )
            
            return json.load(open(file_path))
        
        for iens in range(ensemble.ensemble_size):
            ref_file = get_params_file(reference_folder, iens)
            generated_file = get_params_file(runpath_folder, iens)
            
            assert ref_file == generated_file
"""
        )

    parsed_args = ert_parser(
        ArgumentParser(prog="snake_oil.ert"), ["es_mda", "snake_oil.ert"]
    )

    run_cli(parsed_args)

    # Reference test data copied after successful correct run with:
    # rsync -aq --prune-empty-dirs \
    #   storage/snake_oil/runpath/realization-* expected_parameters \
    #   --include="*/" \
    #   --include="parameters.json" \
    #   --exclude="*"
