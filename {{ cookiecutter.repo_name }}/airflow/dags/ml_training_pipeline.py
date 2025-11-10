"""
Airflow DAG for ML Training Pipeline

This DAG orchestrates the complete ML training workflow:
1. Data extraction from S3/Athena
2. Feature engineering
3. Model training
4. Model evaluation
5. Model registration to MLFlow
6. Model promotion to production
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    "owner": "{{ cookiecutter.author_name }}",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    "ml_training_pipeline",
    default_args=default_args,
    description="Complete ML training pipeline with MLFlow integration",
    schedule_interval="0 2 * * *",  # Daily at 2 AM
    catchup=False,
    tags=["ml", "training", "mlflow"],
)


def extract_features(**context):
    """Extract features from data lake."""
    from src.data.aws_integration import get_athena_client, get_iceberg_manager

    athena = get_athena_client()
    iceberg = get_iceberg_manager()

    # Query features from Iceberg tables
    features_df = athena.query(
        """
        SELECT *
        FROM feature_store
        WHERE date >= date_add('day', -30, current_date)
        """
    )

    # Save to S3
    s3_path = f"s3://{os.getenv('S3_PROCESSED_BUCKET')}/ml/features/features_{datetime.now().strftime('%Y%m%d')}.parquet"
    iceberg.s3.write_parquet(features_df, s3_path.replace("s3://", "").split("/", 1)[1])

    return s3_path


def prepare_training_data(**context):
    """Prepare training dataset."""
    from src.data.aws_integration import get_s3_client

    s3 = get_s3_client()

    # Get features path from previous task
    features_path = context["ti"].xcom_pull(task_ids="extract_features")

    # Load features
    features_df = s3.read_parquet(features_path.replace("s3://", "").split("/", 1)[1])

    # Feature engineering and preprocessing
    # TODO: Implement feature engineering logic

    # Split train/test
    from sklearn.model_selection import train_test_split

    train_df, test_df = train_test_split(features_df, test_size=0.2, random_state=42)

    # Save training datasets
    train_path = f"ml/training_sets/train_{datetime.now().strftime('%Y%m%d')}.parquet"
    test_path = f"ml/training_sets/test_{datetime.now().strftime('%Y%m%d')}.parquet"

    s3.write_parquet(train_df, train_path)
    s3.write_parquet(test_df, test_path)

    return {"train_path": train_path, "test_path": test_path}


def train_model(**context):
    """Train ML model with MLFlow tracking."""
    from src.data.aws_integration import get_s3_client
    from src.model.mlflow_manager import MLFlowManager, autolog

    s3 = get_s3_client()
    mlflow_manager = MLFlowManager()

    # Get training data paths
    paths = context["ti"].xcom_pull(task_ids="prepare_training_data")
    train_df = s3.read_parquet(paths["train_path"])
    test_df = s3.read_parquet(paths["test_path"])

    # Enable autologging
    autolog("sklearn")

    # Start MLFlow run
    with mlflow_manager.start_run(run_name=f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Prepare features and target
        # TODO: Configure feature columns and target
        feature_columns = [col for col in train_df.columns if col != "target"]
        X_train = train_df[feature_columns]
        y_train = train_df["target"]
        X_test = test_df[feature_columns]
        y_test = test_df["target"]

        # Train model
        from sklearn.ensemble import RandomForestClassifier

        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Log additional metrics
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

        y_pred = model.predict(X_test)
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred, average="weighted"),
            "recall": recall_score(y_test, y_pred, average="weighted"),
            "f1": f1_score(y_test, y_pred, average="weighted"),
        }

        mlflow_manager.log_metrics(metrics)

        # Log model
        mlflow_manager.log_model(
            model=model,
            artifact_path="model",
            registered_model_name="{{ cookiecutter.project_name }}_model",
        )

        return {"metrics": metrics, "run_id": mlflow_manager.client.get_run(mlflow_manager.client.list_run_infos(mlflow_manager.experiment_id)[0].run_id).info.run_id}


def evaluate_model(**context):
    """Evaluate model performance."""
    from src.model.mlflow_manager import MLFlowManager

    mlflow_manager = MLFlowManager()

    # Get training results
    training_results = context["ti"].xcom_pull(task_ids="train_model")
    metrics = training_results["metrics"]

    # Define thresholds
    ACCURACY_THRESHOLD = 0.7
    F1_THRESHOLD = 0.65

    # Check if model meets requirements
    if metrics["accuracy"] >= ACCURACY_THRESHOLD and metrics["f1"] >= F1_THRESHOLD:
        return {"approved": True, "metrics": metrics}
    else:
        return {"approved": False, "metrics": metrics}


def promote_model(**context):
    """Promote model to production if approved."""
    from src.model.mlflow_manager import MLFlowManager

    mlflow_manager = MLFlowManager()

    # Get evaluation results
    evaluation = context["ti"].xcom_pull(task_ids="evaluate_model")

    if not evaluation["approved"]:
        raise ValueError("Model did not meet performance thresholds")

    # Get latest model version
    model_name = "{{ cookiecutter.project_name }}_model"
    latest_version = mlflow_manager.get_latest_model_version(model_name)

    # Promote to production
    mlflow_manager.transition_model_stage(
        name=model_name,
        version=latest_version.version,
        stage="Production",
        archive_existing_versions=True,
    )

    return {"model_name": model_name, "version": latest_version.version}


def send_notification(**context):
    """Send notification about pipeline completion."""
    # TODO: Implement notification logic (email, Slack, etc.)
    print("Training pipeline completed successfully!")


# Define tasks
with dag:
    # Task 1: Wait for new data
    wait_for_data = S3KeySensor(
        task_id="wait_for_data",
        bucket_name=os.getenv("S3_PROCESSED_BUCKET"),
        bucket_key="processed/silver/",
        aws_conn_id="aws_default",
        timeout=3600,
        poke_interval=300,
    )

    # Task 2: Extract features
    extract_features_task = PythonOperator(
        task_id="extract_features",
        python_callable=extract_features,
        provide_context=True,
    )

    # Task 3: Prepare training data
    prepare_data_task = PythonOperator(
        task_id="prepare_training_data",
        python_callable=prepare_training_data,
        provide_context=True,
    )

    # Task 4: Train model
    train_model_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
        provide_context=True,
    )

    # Task 5: Evaluate model
    evaluate_model_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
        provide_context=True,
    )

    # Task 6: Promote model
    promote_model_task = PythonOperator(
        task_id="promote_model",
        python_callable=promote_model,
        provide_context=True,
    )

    # Task 7: Send notification
    notify_task = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
        provide_context=True,
    )

    # Define task dependencies
    (
        wait_for_data
        >> extract_features_task
        >> prepare_data_task
        >> train_model_task
        >> evaluate_model_task
        >> promote_model_task
        >> notify_task
    )
