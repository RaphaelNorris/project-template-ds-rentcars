"""
Airflow DAG for ML Batch Inference Pipeline

This DAG orchestrates batch prediction workflow:
1. Load production data
2. Load production model from MLFlow
3. Generate predictions
4. Save predictions to S3/Iceberg
5. Monitor predictions for drift
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments
default_args = {
    "owner": "{{ cookiecutter.author_name }}",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    "ml_batch_inference_pipeline",
    default_args=default_args,
    description="Batch inference pipeline with drift monitoring",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    catchup=False,
    tags=["ml", "inference", "batch"],
)


def load_inference_data(**context):
    """Load data for batch inference."""
    from src.data.aws_integration import get_athena_client

    athena = get_athena_client()

    # Query data for inference
    inference_df = athena.query(
        """
        SELECT *
        FROM feature_store
        WHERE date = current_date
        AND prediction_timestamp IS NULL
        """
    )

    if len(inference_df) == 0:
        raise ValueError("No data available for inference")

    # Save to temporary location
    execution_date = context["execution_date"].strftime("%Y%m%d_%H%M%S")
    temp_path = f"ml/temp/inference_data_{execution_date}.parquet"

    from src.data.aws_integration import get_s3_client

    s3 = get_s3_client()
    s3.write_parquet(inference_df, temp_path)

    return temp_path


def load_production_model(**context):
    """Load production model from MLFlow."""
    from src.model.mlflow_manager import MLFlowManager

    mlflow_manager = MLFlowManager()

    # Get production model
    model_name = "{{ cookiecutter.project_name }}_model"
    model_version = mlflow_manager.get_latest_model_version(
        name=model_name,
        stage="Production",
    )

    if model_version is None:
        raise ValueError(f"No production model found: {model_name}")

    # Load model
    model_uri = f"models:/{model_name}/Production"
    model = mlflow_manager.load_model(model_uri)

    return {
        "model_name": model_name,
        "model_version": model_version.version,
        "model_uri": model_uri,
    }


def generate_predictions(**context):
    """Generate batch predictions."""
    import mlflow
    from src.data.aws_integration import get_s3_client

    s3 = get_s3_client()

    # Load data
    data_path = context["ti"].xcom_pull(task_ids="load_inference_data")
    inference_df = s3.read_parquet(data_path)

    # Load model
    model_info = context["ti"].xcom_pull(task_ids="load_production_model")
    model = mlflow.pyfunc.load_model(model_info["model_uri"])

    # Generate predictions
    feature_columns = [col for col in inference_df.columns if col not in ["id", "date", "target"]]
    X = inference_df[feature_columns]

    predictions = model.predict(X)

    # Add predictions to dataframe
    inference_df["prediction"] = predictions
    inference_df["prediction_timestamp"] = datetime.now()
    inference_df["model_version"] = model_info["model_version"]

    # Save predictions
    execution_date = context["execution_date"].strftime("%Y%m%d_%H%M%S")
    predictions_path = f"ml/predictions/predictions_{execution_date}.parquet"
    s3.write_parquet(inference_df, predictions_path)

    return {
        "predictions_path": predictions_path,
        "num_predictions": len(predictions),
        "model_version": model_info["model_version"],
    }


def save_to_iceberg(**context):
    """Save predictions to Iceberg table."""
    from src.data.aws_integration import get_s3_client, get_iceberg_manager

    s3 = get_s3_client()
    iceberg = get_iceberg_manager()

    # Load predictions
    predictions_info = context["ti"].xcom_pull(task_ids="generate_predictions")
    predictions_df = s3.read_parquet(predictions_info["predictions_path"])

    # Write to Iceberg
    iceberg.write_features(
        df=predictions_df,
        table_name="model_predictions",
        mode="append",
    )

    return {"status": "success", "rows_written": len(predictions_df)}


def monitor_predictions(**context):
    """Monitor predictions for data drift."""
    from src.data.aws_integration import get_s3_client
    from src.inference.monitoring import create_monitor, create_alert_manager

    s3 = get_s3_client()
    monitor = create_monitor(
        reference_data_path=os.getenv("REFERENCE_DATA_PATH"),
        drift_threshold=0.05,
    )
    alert_manager = create_alert_manager()

    # Load predictions
    predictions_info = context["ti"].xcom_pull(task_ids="generate_predictions")
    predictions_df = s3.read_parquet(predictions_info["predictions_path"])

    # Detect drift
    drift_results = monitor.detect_data_drift(current_data=predictions_df)

    if drift_results["drift_detected"]:
        alert_manager.send_alert(
            alert_type="data_drift",
            message=f"Data drift detected in batch inference on {datetime.now().strftime('%Y-%m-%d')}",
            severity="warning",
            metadata=drift_results,
        )

    return drift_results


def check_drift_status(**context):
    """Check if drift was detected and branch accordingly."""
    drift_results = context["ti"].xcom_pull(task_ids="monitor_predictions")

    if drift_results["drift_detected"]:
        return "handle_drift"
    else:
        return "complete_pipeline"


def handle_drift(**context):
    """Handle detected drift."""
    # TODO: Implement drift handling logic
    # Options:
    # - Trigger model retraining
    # - Send notifications to data science team
    # - Update monitoring thresholds
    print("Drift detected - handling required")


def complete_pipeline(**context):
    """Complete pipeline successfully."""
    predictions_info = context["ti"].xcom_pull(task_ids="generate_predictions")
    print(f"Batch inference completed: {predictions_info['num_predictions']} predictions generated")


# Define tasks
with dag:
    # Task 1: Load inference data
    load_data_task = PythonOperator(
        task_id="load_inference_data",
        python_callable=load_inference_data,
        provide_context=True,
    )

    # Task 2: Load production model
    load_model_task = PythonOperator(
        task_id="load_production_model",
        python_callable=load_production_model,
        provide_context=True,
    )

    # Task 3: Generate predictions
    predict_task = PythonOperator(
        task_id="generate_predictions",
        python_callable=generate_predictions,
        provide_context=True,
    )

    # Task 4: Save to Iceberg
    save_task = PythonOperator(
        task_id="save_to_iceberg",
        python_callable=save_to_iceberg,
        provide_context=True,
    )

    # Task 5: Monitor predictions
    monitor_task = PythonOperator(
        task_id="monitor_predictions",
        python_callable=monitor_predictions,
        provide_context=True,
    )

    # Task 6: Check drift and branch
    check_drift_task = BranchPythonOperator(
        task_id="check_drift_status",
        python_callable=check_drift_status,
        provide_context=True,
    )

    # Task 7a: Handle drift
    handle_drift_task = PythonOperator(
        task_id="handle_drift",
        python_callable=handle_drift,
        provide_context=True,
    )

    # Task 7b: Complete pipeline
    complete_task = PythonOperator(
        task_id="complete_pipeline",
        python_callable=complete_pipeline,
        provide_context=True,
    )

    # Define task dependencies
    load_data_task >> load_model_task >> predict_task >> save_task >> monitor_task
    monitor_task >> check_drift_task >> [handle_drift_task, complete_task]
