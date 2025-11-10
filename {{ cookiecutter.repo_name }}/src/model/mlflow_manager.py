"""
MLFlow Manager for MLOps Pipeline

This module provides utilities for:
- Experiment tracking
- Model logging and versioning
- Model registry management
- Artifact storage
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import mlflow
import mlflow.sklearn
import mlflow.xgboost
import mlflow.lightgbm
from loguru import logger
from mlflow.entities import Run
from mlflow.tracking import MlflowClient


class MLFlowManager:
    """Manager for MLFlow tracking and model registry."""

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        experiment_name: Optional[str] = None,
        artifact_location: Optional[str] = None,
    ):
        """
        Initialize MLFlow manager.

        Args:
            tracking_uri: MLFlow tracking server URI
            experiment_name: Name of MLFlow experiment
            artifact_location: S3 location for artifacts
        """
        self.tracking_uri = tracking_uri or os.getenv(
            "MLFLOW_TRACKING_URI",
            "http://localhost:5000",
        )
        self.experiment_name = experiment_name or os.getenv(
            "MLFLOW_EXPERIMENT_NAME",
            "default",
        )
        self.artifact_location = artifact_location or os.getenv(
            "MLFLOW_ARTIFACT_LOCATION",
        )

        # Configure MLFlow
        mlflow.set_tracking_uri(self.tracking_uri)

        # Create or get experiment
        try:
            experiment = mlflow.get_experiment_by_name(self.experiment_name)
            if experiment is None:
                experiment_id = mlflow.create_experiment(
                    name=self.experiment_name,
                    artifact_location=self.artifact_location,
                )
                logger.info(f"Created experiment: {self.experiment_name}")
            else:
                experiment_id = experiment.experiment_id
                logger.info(f"Using existing experiment: {self.experiment_name}")

            self.experiment_id = experiment_id
            mlflow.set_experiment(self.experiment_name)
        except Exception as e:
            logger.error(f"Failed to set up experiment: {e}")
            raise

        self.client = MlflowClient(tracking_uri=self.tracking_uri)

    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, Any]] = None,
        nested: bool = False,
    ) -> mlflow.ActiveRun:
        """
        Start a new MLFlow run.

        Args:
            run_name: Name for the run
            tags: Dictionary of tags
            nested: Whether this is a nested run

        Returns:
            Active run context
        """
        return mlflow.start_run(
            run_name=run_name,
            experiment_id=self.experiment_id,
            tags=tags,
            nested=nested,
        )

    def log_params(self, params: Dict[str, Any]) -> None:
        """
        Log parameters to current run.

        Args:
            params: Dictionary of parameters
        """
        try:
            mlflow.log_params(params)
            logger.debug(f"Logged {len(params)} parameters")
        except Exception as e:
            logger.error(f"Failed to log parameters: {e}")
            raise

    def log_metrics(
        self,
        metrics: Dict[str, float],
        step: Optional[int] = None,
    ) -> None:
        """
        Log metrics to current run.

        Args:
            metrics: Dictionary of metrics
            step: Step number for metric
        """
        try:
            mlflow.log_metrics(metrics, step=step)
            logger.debug(f"Logged {len(metrics)} metrics")
        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
            raise

    def log_artifact(
        self,
        local_path: Union[str, Path],
        artifact_path: Optional[str] = None,
    ) -> None:
        """
        Log artifact to current run.

        Args:
            local_path: Path to local file
            artifact_path: Path within artifact directory
        """
        try:
            mlflow.log_artifact(str(local_path), artifact_path)
            logger.debug(f"Logged artifact: {local_path}")
        except Exception as e:
            logger.error(f"Failed to log artifact: {e}")
            raise

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        registered_model_name: Optional[str] = None,
        signature: Optional[Any] = None,
        input_example: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        """
        Log model to current run.

        Args:
            model: Trained model object
            artifact_path: Path for model artifacts
            registered_model_name: Name for model registry
            signature: Model signature
            input_example: Example input for model
            **kwargs: Additional arguments
        """
        try:
            # Detect model type and use appropriate logger
            model_type = type(model).__name__

            if "sklearn" in str(type(model).__module__):
                mlflow.sklearn.log_model(
                    sk_model=model,
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example,
                    **kwargs,
                )
            elif "xgboost" in str(type(model).__module__):
                mlflow.xgboost.log_model(
                    xgb_model=model,
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example,
                    **kwargs,
                )
            elif "lightgbm" in str(type(model).__module__):
                mlflow.lightgbm.log_model(
                    lgb_model=model,
                    artifact_path=artifact_path,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example,
                    **kwargs,
                )
            else:
                mlflow.pyfunc.log_model(
                    artifact_path=artifact_path,
                    python_model=model,
                    registered_model_name=registered_model_name,
                    signature=signature,
                    input_example=input_example,
                    **kwargs,
                )

            logger.info(f"Logged {model_type} model to {artifact_path}")
        except Exception as e:
            logger.error(f"Failed to log model: {e}")
            raise

    def load_model(
        self,
        model_uri: str,
    ) -> Any:
        """
        Load model from MLFlow.

        Args:
            model_uri: URI of the model (e.g., 'models:/model_name/version')

        Returns:
            Loaded model
        """
        try:
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(f"Loaded model from {model_uri}")
            return model
        except Exception as e:
            logger.error(f"Failed to load model from {model_uri}: {e}")
            raise

    def register_model(
        self,
        model_uri: str,
        name: str,
        tags: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None,
    ) -> Any:
        """
        Register model to MLFlow Model Registry.

        Args:
            model_uri: URI of the model
            name: Name for registered model
            tags: Dictionary of tags
            description: Model description

        Returns:
            Registered model version
        """
        try:
            registered_model = mlflow.register_model(model_uri, name)

            if tags:
                for key, value in tags.items():
                    self.client.set_model_version_tag(
                        name=name,
                        version=registered_model.version,
                        key=key,
                        value=value,
                    )

            if description:
                self.client.update_model_version(
                    name=name,
                    version=registered_model.version,
                    description=description,
                )

            logger.info(f"Registered model: {name} version {registered_model.version}")
            return registered_model
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise

    def transition_model_stage(
        self,
        name: str,
        version: Union[int, str],
        stage: str,
        archive_existing_versions: bool = False,
    ) -> None:
        """
        Transition model to a different stage.

        Args:
            name: Model name
            version: Model version
            stage: Target stage ('Staging', 'Production', 'Archived')
            archive_existing_versions: Archive existing versions in target stage
        """
        try:
            self.client.transition_model_version_stage(
                name=name,
                version=str(version),
                stage=stage,
                archive_existing_versions=archive_existing_versions,
            )
            logger.info(f"Transitioned {name} v{version} to {stage}")
        except Exception as e:
            logger.error(f"Failed to transition model stage: {e}")
            raise

    def get_latest_model_version(
        self,
        name: str,
        stage: Optional[str] = None,
    ) -> Optional[Any]:
        """
        Get latest model version.

        Args:
            name: Model name
            stage: Model stage filter ('Staging', 'Production', 'Archived')

        Returns:
            Latest model version
        """
        try:
            if stage:
                versions = self.client.get_latest_versions(name, stages=[stage])
            else:
                versions = self.client.search_model_versions(f"name='{name}'")

            if versions:
                latest = max(versions, key=lambda v: int(v.version))
                logger.info(f"Latest version of {name}: {latest.version}")
                return latest
            return None
        except Exception as e:
            logger.error(f"Failed to get latest model version: {e}")
            raise

    def compare_runs(
        self,
        run_ids: List[str],
        metrics: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Compare multiple runs.

        Args:
            run_ids: List of run IDs to compare
            metrics: List of metrics to compare

        Returns:
            Dictionary with comparison data
        """
        comparison = {"runs": {}}

        for run_id in run_ids:
            try:
                run = self.client.get_run(run_id)
                run_data = {
                    "params": run.data.params,
                    "metrics": run.data.metrics,
                    "tags": run.data.tags,
                }

                if metrics:
                    run_data["metrics"] = {
                        k: v for k, v in run.data.metrics.items() if k in metrics
                    }

                comparison["runs"][run_id] = run_data
            except Exception as e:
                logger.error(f"Failed to get run {run_id}: {e}")

        return comparison

    def search_runs(
        self,
        filter_string: str = "",
        order_by: Optional[List[str]] = None,
        max_results: int = 100,
    ) -> List[Run]:
        """
        Search runs in experiment.

        Args:
            filter_string: Filter query string
            order_by: List of order by clauses
            max_results: Maximum number of results

        Returns:
            List of matching runs
        """
        try:
            runs = self.client.search_runs(
                experiment_ids=[self.experiment_id],
                filter_string=filter_string,
                order_by=order_by,
                max_results=max_results,
            )
            logger.info(f"Found {len(runs)} runs matching filter")
            return runs
        except Exception as e:
            logger.error(f"Failed to search runs: {e}")
            raise

    def delete_run(self, run_id: str) -> None:
        """
        Delete a run.

        Args:
            run_id: ID of run to delete
        """
        try:
            self.client.delete_run(run_id)
            logger.info(f"Deleted run: {run_id}")
        except Exception as e:
            logger.error(f"Failed to delete run {run_id}: {e}")
            raise


# Convenience functions
def get_mlflow_manager(**kwargs: Any) -> MLFlowManager:
    """Get configured MLFlow manager."""
    return MLFlowManager(**kwargs)


def autolog(framework: str = "sklearn") -> None:
    """
    Enable MLFlow autologging for a framework.

    Args:
        framework: Framework name ('sklearn', 'xgboost', 'lightgbm', etc.)
    """
    if framework == "sklearn":
        mlflow.sklearn.autolog()
    elif framework == "xgboost":
        mlflow.xgboost.autolog()
    elif framework == "lightgbm":
        mlflow.lightgbm.autolog()
    else:
        logger.warning(f"Autolog not available for {framework}")

    logger.info(f"Enabled autologging for {framework}")
