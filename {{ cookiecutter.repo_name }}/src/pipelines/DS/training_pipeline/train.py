"""
Complete ML Training Pipeline

This module implements an end-to-end training pipeline with:
- Feature loading from S3/Athena
- Data preprocessing and validation
- Model training with hyperparameter tuning
- Model evaluation
- MLFlow tracking and model registry
- Model deployment
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from loguru import logger
from sklearn.model_selection import train_test_split

from src.data.aws_integration import get_athena_client, get_iceberg_manager, get_s3_client
from src.model.mlflow_manager import MLFlowManager, autolog


class TrainingPipeline:
    """Complete ML training pipeline."""

    def __init__(
        self,
        experiment_name: Optional[str] = None,
        model_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize training pipeline.

        Args:
            experiment_name: MLFlow experiment name
            experiment_name: MLFlow experiment name
            model_name: Name for model registry
            config: Pipeline configuration
        """
        self.config = config or {}
        self.experiment_name = experiment_name or os.getenv(
            "MLFLOW_EXPERIMENT_NAME",
            "default",
        )
        self.model_name = model_name or "{{ cookiecutter.project_name }}_model"

        # Initialize components
        self.s3 = get_s3_client()
        self.athena = get_athena_client()
        self.iceberg = get_iceberg_manager()
        self.mlflow = MLFlowManager(experiment_name=self.experiment_name)

        logger.info(f"Initialized training pipeline: {self.experiment_name}")

    def load_features(
        self,
        query: Optional[str] = None,
        s3_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load features from Athena or S3.

        Args:
            query: SQL query for Athena
            s3_path: S3 path to parquet file

        Returns:
            Features DataFrame
        """
        logger.info("Loading features...")

        if query:
            df = self.athena.query(query)
        elif s3_path:
            df = self.s3.read_parquet(s3_path)
        else:
            # Default: load from feature store
            df = self.iceberg.read_features("feature_store")

        logger.info(f"Loaded {len(df)} samples with {len(df.columns)} features")
        return df

    def preprocess_data(
        self,
        df: pd.DataFrame,
        target_column: str = "target",
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Preprocess data for training.

        Args:
            df: Input DataFrame
            target_column: Name of target column

        Returns:
            Tuple of (features, target)
        """
        logger.info("Preprocessing data...")

        # Separate features and target
        X = df.drop(columns=[target_column])
        y = df[target_column]

        # Handle missing values
        X = X.fillna(X.mean(numeric_only=True))

        # TODO: Add more preprocessing steps
        # - Feature encoding
        # - Feature scaling
        # - Feature selection

        logger.info(f"Preprocessed data: {X.shape}")
        return X, y

    def split_data(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        test_size: float = 0.2,
        random_state: int = 42,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Split data into train and test sets.

        Args:
            X: Features
            y: Target
            test_size: Proportion of test set
            random_state: Random seed

        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        logger.info("Splitting data...")

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=random_state,
            stratify=y if len(y.unique()) > 1 else None,
        )

        logger.info(f"Train set: {len(X_train)}, Test set: {len(X_test)}")
        return X_train, X_test, y_train, y_test

    def train_model(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        model_type: str = "random_forest",
        hyperparameters: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Train ML model.

        Args:
            X_train: Training features
            y_train: Training target
            model_type: Type of model to train
            hyperparameters: Model hyperparameters

        Returns:
            Trained model
        """
        logger.info(f"Training {model_type} model...")

        # Enable autologging
        autolog("sklearn")

        # Select model
        if model_type == "random_forest":
            from sklearn.ensemble import RandomForestClassifier

            model = RandomForestClassifier(**(hyperparameters or {}))
        elif model_type == "xgboost":
            from xgboost import XGBClassifier

            model = XGBClassifier(**(hyperparameters or {}))
        elif model_type == "lightgbm":
            from lightgbm import LGBMClassifier

            model = LGBMClassifier(**(hyperparameters or {}))
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        # Train model
        model.fit(X_train, y_train)

        logger.info("Model training completed")
        return model

    def evaluate_model(
        self,
        model: Any,
        X_test: pd.DataFrame,
        y_test: pd.Series,
    ) -> Dict[str, float]:
        """
        Evaluate model performance.

        Args:
            model: Trained model
            X_test: Test features
            y_test: Test target

        Returns:
            Dictionary of metrics
        """
        logger.info("Evaluating model...")

        from sklearn.metrics import (
            accuracy_score,
            f1_score,
            precision_score,
            recall_score,
            roc_auc_score,
        )

        # Generate predictions
        y_pred = model.predict(X_test)
        y_pred_proba = (
            model.predict_proba(X_test)[:, 1]
            if hasattr(model, "predict_proba")
            else y_pred
        )

        # Calculate metrics
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, average="weighted", zero_division=0),
            "recall": recall_score(y_test, y_pred, average="weighted", zero_division=0),
            "f1": f1_score(y_test, y_pred, average="weighted", zero_division=0),
        }

        # Add AUC if binary classification
        if len(y_test.unique()) == 2:
            metrics["roc_auc"] = roc_auc_score(y_test, y_pred_proba)

        logger.info(f"Model metrics: {metrics}")
        return metrics

    def run(
        self,
        query: Optional[str] = None,
        s3_path: Optional[str] = None,
        model_type: str = "random_forest",
        hyperparameters: Optional[Dict[str, Any]] = None,
        register_model: bool = True,
    ) -> Dict[str, Any]:
        """
        Run complete training pipeline.

        Args:
            query: SQL query for feature loading
            s3_path: S3 path for feature loading
            model_type: Type of model to train
            hyperparameters: Model hyperparameters
            register_model: Whether to register model in MLFlow

        Returns:
            Pipeline results
        """
        with self.mlflow.start_run(run_name=f"training_{model_type}"):
            # Log pipeline parameters
            self.mlflow.log_params(
                {
                    "model_type": model_type,
                    "data_source": "athena" if query else "s3",
                    **(hyperparameters or {}),
                }
            )

            # Step 1: Load features
            df = self.load_features(query=query, s3_path=s3_path)

            # Step 2: Preprocess data
            X, y = self.preprocess_data(df)

            # Step 3: Split data
            X_train, X_test, y_train, y_test = self.split_data(X, y)

            # Step 4: Train model
            model = self.train_model(X_train, y_train, model_type, hyperparameters)

            # Step 5: Evaluate model
            metrics = self.evaluate_model(model, X_test, y_test)

            # Log metrics
            self.mlflow.log_metrics(metrics)

            # Step 6: Log model
            self.mlflow.log_model(
                model=model,
                artifact_path="model",
                registered_model_name=self.model_name if register_model else None,
            )

            logger.info("Training pipeline completed successfully")

            return {
                "model_name": self.model_name,
                "metrics": metrics,
                "num_features": X_train.shape[1],
                "num_samples": len(df),
            }


def main():
    """Main entry point for training pipeline."""
    # Example configuration
    config = {
        "model_type": "random_forest",
        "hyperparameters": {
            "n_estimators": 100,
            "max_depth": 10,
            "random_state": 42,
        },
    }

    # Initialize and run pipeline
    pipeline = TrainingPipeline(
        experiment_name="production_training",
        model_name="{{ cookiecutter.project_name }}_model",
        config=config,
    )

    # Run training
    results = pipeline.run(
        # query="SELECT * FROM feature_store WHERE date >= date_add('day', -30, current_date)",
        s3_path="ml/features/features_latest.parquet",
        model_type=config["model_type"],
        hyperparameters=config["hyperparameters"],
        register_model=True,
    )

    logger.info(f"Training completed: {results}")


if __name__ == "__main__":
    main()
