from __future__ import annotations

import logging
from queue import SimpleQueue
from typing import TYPE_CHECKING, List
from uuid import UUID

import numpy as np

from ert.config import ErtConfig
from ert.enkf_main import sample_prior
from ert.ensemble_evaluator import EvaluatorServerConfig
from ert.run_models.run_arguments import ESMDARunArguments
from ert.storage import Ensemble, Storage

from ..config.analysis_config import UpdateSettings
from ..config.analysis_module import ESSettings
from ..run_arg import create_run_arguments
from .base_run_model import ErtRunError, StatusEvents, UpdateRunModel

if TYPE_CHECKING:
    from ert.config import QueueConfig

logger = logging.getLogger(__file__)


class MultipleDataAssimilation(UpdateRunModel):
    """
    Run multiple data assimilation (MDA) ensemble smoother with custom weights.
    """

    default_weights = "4, 2, 1"

    def __init__(
        self,
        simulation_arguments: ESMDARunArguments,
        config: ErtConfig,
        storage: Storage,
        queue_config: QueueConfig,
        es_settings: ESSettings,
        update_settings: UpdateSettings,
        status_queue: SimpleQueue[StatusEvents],
    ):
        self.weights = self.parse_weights(simulation_arguments.weights)

        self.target_ensemble_format = simulation_arguments.target_ensemble
        self.experiment_name = simulation_arguments.experiment_name
        self.restart_run = simulation_arguments.restart_run
        self.prior_ensemble_id = simulation_arguments.prior_ensemble_id
        if self.restart_run:
            if not self.prior_ensemble_id:
                raise ValueError("For restart run, prior ensemble must be set")
        elif not self.experiment_name:
            raise ValueError("For non-restart run, experiment name must be set")
        super().__init__(
            es_settings,
            update_settings,
            config,
            storage,
            queue_config,
            status_queue,
            active_realizations=simulation_arguments.active_realizations,
            total_iterations=len(self.weights) + 1,
            start_iteration=simulation_arguments.starting_iteration,
            random_seed=simulation_arguments.random_seed,
            minimum_required_realizations=simulation_arguments.minimum_required_realizations,
        )

    def run_experiment(
        self, evaluator_server_config: EvaluatorServerConfig, restart: bool = False
    ) -> None:
        log_msg = f"Running ES-MDA with normalized weights {self.weights}"
        logger.info(log_msg)
        self._current_iteration_label = log_msg

        if self.restart_run:
            id = self.prior_ensemble_id
            try:
                ensemble_id = UUID(id)
                prior = self._storage.get_ensemble(ensemble_id)
                experiment = prior.experiment
                self.set_env_key("_ERT_EXPERIMENT_ID", str(experiment.id))
                self.set_env_key("_ERT_ENSEMBLE_ID", str(prior.id))
                assert isinstance(prior, Ensemble)
                if self.start_iteration != prior.iteration + 1:
                    raise ValueError(
                        f"Experiment misconfigured, got starting iteration: {self.start_iteration},"
                        f"restart iteration = {prior.iteration + 1}"
                    )
            except (KeyError, ValueError) as err:
                raise ErtRunError(
                    f"Prior ensemble with ID: {id} does not exists"
                ) from err
        else:
            experiment = self._storage.create_experiment(
                parameters=self.ert_config.ensemble_config.parameter_configuration,
                observations=self.ert_config.observations,
                responses=self.ert_config.ensemble_config.response_configuration,
                name=self.experiment_name,
            )

            prior = self._storage.create_ensemble(
                experiment,
                ensemble_size=self.ensemble_size,
                iteration=0,
                name=self.target_ensemble_format % 0,
            )
            self.set_env_key("_ERT_EXPERIMENT_ID", str(experiment.id))
            self.set_env_key("_ERT_ENSEMBLE_ID", str(prior.id))
            prior_args = create_run_arguments(
                self.run_paths,
                np.array(self.active_realizations, dtype=bool),
                ensemble=prior,
            )
            sample_prior(
                prior,
                np.where(self.active_realizations)[0],
                random_seed=self.random_seed,
            )
            self._evaluate_and_postprocess(
                prior_args,
                prior,
                evaluator_server_config,
            )
        enumerated_weights = list(enumerate(self.weights))
        weights_to_run = enumerated_weights[prior.iteration :]

        for iteration, weight in weights_to_run:
            posterior = self.update(
                prior,
                self.target_ensemble_format % (iteration + 1),
                weight=weight,
            )
            posterior_args = create_run_arguments(
                self.run_paths,
                self.active_realizations,
                ensemble=posterior,
            )
            self._evaluate_and_postprocess(
                posterior_args,
                posterior,
                evaluator_server_config,
            )
            prior = posterior

    @staticmethod
    def parse_weights(weights: str) -> List[float]:
        """Parse weights string and scale weights such that their reciprocals sum
        to 1.0, i.e., sum(1.0 / x for x in weights) == 1.0. See for example Equation
        38 of evensen2018 - Analysis of iterative ensemble
        smoothers for solving inverse problems.
        """
        if not weights:
            raise ValueError(f"Must provide weights, got {weights}")

        elements = weights.split(",")
        elements = [element.strip() for element in elements if element.strip()]

        result: List[float] = []
        for element in elements:
            try:
                f = float(element)
                if f == 0:
                    logger.info("Warning: 0 weight, will ignore")
                else:
                    result.append(f)
            except ValueError as e:
                raise ValueError(f"Warning: cannot parse weight {element}") from e
        if not result:
            raise ValueError(f"Invalid weights: {weights}")

        length = sum(1.0 / x for x in result)
        return [x * length for x in result]

    @classmethod
    def name(cls) -> str:
        return "Multiple Data Assimilation (ES MDA) - Recommended"

    @classmethod
    def description(cls) -> str:
        return "[Sample|restart] → [Evaluate → update] for each weight"
