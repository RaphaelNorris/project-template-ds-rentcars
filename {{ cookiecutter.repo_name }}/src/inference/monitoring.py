"""
Model Monitoring Module for MLOps Pipeline

This module provides:
- Data drift detection
- Model performance monitoring
- Prediction logging
- Alert generation
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from evidently import ColumnMapping
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
from evidently.report import Report
from loguru import logger


class ModelMonitor:
    """Monitor for model predictions and data drift."""

    def __init__(
        self,
        reference_data_path: Optional[str] = None,
        drift_threshold: float = 0.05,
        output_dir: Optional[str] = None,
    ):
        """
        Initialize model monitor.

        Args:
            reference_data_path: Path to reference/training data
            drift_threshold: Threshold for drift detection
            output_dir: Directory for monitoring outputs
        """
        self.drift_threshold = drift_threshold
        self.output_dir = Path(output_dir or "data/ml/monitoring")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.reference_data = None
        if reference_data_path:
            self.load_reference_data(reference_data_path)

        self.predictions_log = []

    def load_reference_data(self, path: str) -> None:
        """
        Load reference data for comparison.

        Args:
            path: Path to reference data
        """
        try:
            self.reference_data = pd.read_parquet(path)
            logger.info(f"Loaded reference data: {len(self.reference_data)} rows")
        except Exception as e:
            logger.error(f"Failed to load reference data: {e}")
            raise

    def log_prediction(
        self,
        features: pd.DataFrame,
        predictions: List[Any],
        model_version: Optional[str] = None,
    ) -> None:
        """
        Log prediction for monitoring.

        Args:
            features: Input features
            predictions: Model predictions
            model_version: Version of model used
        """
        timestamp = datetime.now()

        prediction_log = {
            "timestamp": timestamp,
            "model_version": model_version,
            "num_samples": len(predictions),
            "features": features.copy(),
            "predictions": predictions,
        }

        self.predictions_log.append(prediction_log)

        # Periodically save logs
        if len(self.predictions_log) >= 100:
            self.save_prediction_logs()

    def save_prediction_logs(self) -> None:
        """Save prediction logs to disk."""
        if not self.predictions_log:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = self.output_dir / f"predictions_{timestamp}.parquet"

        try:
            # Combine all logged features
            all_features = pd.concat(
                [log["features"] for log in self.predictions_log],
                ignore_index=True,
            )

            # Add predictions and metadata
            all_features["prediction"] = [
                pred
                for log in self.predictions_log
                for pred in log["predictions"]
            ]
            all_features["model_version"] = [
                log["model_version"]
                for log in self.predictions_log
                for _ in log["predictions"]
            ]
            all_features["timestamp"] = [
                log["timestamp"]
                for log in self.predictions_log
                for _ in log["predictions"]
            ]

            # Save to parquet
            all_features.to_parquet(log_path)
            logger.info(f"Saved {len(all_features)} predictions to {log_path}")

            # Clear logs
            self.predictions_log = []

        except Exception as e:
            logger.error(f"Failed to save prediction logs: {e}")

    def detect_data_drift(
        self,
        current_data: pd.DataFrame,
        reference_data: Optional[pd.DataFrame] = None,
        column_mapping: Optional[ColumnMapping] = None,
    ) -> Dict[str, Any]:
        """
        Detect data drift between reference and current data.

        Args:
            current_data: Current production data
            reference_data: Reference data (uses loaded if not provided)
            column_mapping: Column mapping for Evidently

        Returns:
            Drift detection results
        """
        reference_data = reference_data or self.reference_data

        if reference_data is None:
            raise ValueError("No reference data available for drift detection")

        try:
            # Create drift report
            drift_report = Report(metrics=[DataDriftPreset()])

            drift_report.run(
                reference_data=reference_data,
                current_data=current_data,
                column_mapping=column_mapping,
            )

            # Get drift results
            drift_results = drift_report.as_dict()

            # Check if drift detected
            drift_detected = (
                drift_results.get("metrics", [{}])[0]
                .get("result", {})
                .get("dataset_drift", False)
            )

            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = self.output_dir / f"drift_report_{timestamp}.html"
            drift_report.save_html(str(report_path))

            logger.info(
                f"Drift detection completed. Drift detected: {drift_detected}",
            )

            return {
                "drift_detected": drift_detected,
                "report_path": str(report_path),
                "results": drift_results,
            }

        except Exception as e:
            logger.error(f"Drift detection failed: {e}")
            raise

    def check_data_quality(
        self,
        data: pd.DataFrame,
        reference_data: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Check data quality.

        Args:
            data: Data to check
            reference_data: Reference data for comparison

        Returns:
            Data quality results
        """
        try:
            quality_report = Report(metrics=[DataQualityPreset()])

            quality_report.run(
                reference_data=reference_data or self.reference_data,
                current_data=data,
            )

            # Get quality results
            quality_results = quality_report.as_dict()

            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = self.output_dir / f"quality_report_{timestamp}.html"
            quality_report.save_html(str(report_path))

            logger.info("Data quality check completed")

            return {
                "report_path": str(report_path),
                "results": quality_results,
            }

        except Exception as e:
            logger.error(f"Data quality check failed: {e}")
            raise

    def generate_monitoring_report(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """
        Generate comprehensive monitoring report.

        Args:
            start_date: Start date for report
            end_date: End date for report

        Returns:
            Monitoring report
        """
        # Filter predictions by date range
        filtered_logs = self.predictions_log

        if start_date:
            filtered_logs = [
                log for log in filtered_logs if log["timestamp"] >= start_date
            ]

        if end_date:
            filtered_logs = [
                log for log in filtered_logs if log["timestamp"] <= end_date
            ]

        # Calculate statistics
        total_predictions = sum(log["num_samples"] for log in filtered_logs)
        unique_model_versions = set(log["model_version"] for log in filtered_logs)

        report = {
            "period": {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None,
            },
            "statistics": {
                "total_predictions": total_predictions,
                "total_requests": len(filtered_logs),
                "model_versions": list(unique_model_versions),
            },
        }

        logger.info(f"Generated monitoring report: {report}")

        return report


class AlertManager:
    """Manager for monitoring alerts."""

    def __init__(
        self,
        alert_channels: Optional[List[str]] = None,
    ):
        """
        Initialize alert manager.

        Args:
            alert_channels: List of alert channels (email, slack, etc.)
        """
        self.alert_channels = alert_channels or ["log"]
        self.alerts = []

    def send_alert(
        self,
        alert_type: str,
        message: str,
        severity: str = "warning",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send alert through configured channels.

        Args:
            alert_type: Type of alert (drift, performance, etc.)
            message: Alert message
            severity: Alert severity (info, warning, error, critical)
            metadata: Additional metadata
        """
        alert = {
            "timestamp": datetime.now().isoformat(),
            "type": alert_type,
            "message": message,
            "severity": severity,
            "metadata": metadata or {},
        }

        self.alerts.append(alert)

        # Send through channels
        if "log" in self.alert_channels:
            self._log_alert(alert)

        if "email" in self.alert_channels:
            self._send_email_alert(alert)

        if "slack" in self.alert_channels:
            self._send_slack_alert(alert)

    def _log_alert(self, alert: Dict[str, Any]) -> None:
        """Log alert."""
        logger.log(
            alert["severity"].upper(),
            f"[{alert['type']}] {alert['message']}",
        )

    def _send_email_alert(self, alert: Dict[str, Any]) -> None:
        """Send email alert."""
        # TODO: Implement email alerting
        logger.info(f"Email alert: {alert['message']}")

    def _send_slack_alert(self, alert: Dict[str, Any]) -> None:
        """Send Slack alert."""
        # TODO: Implement Slack alerting
        logger.info(f"Slack alert: {alert['message']}")


# Convenience functions
def create_monitor(**kwargs: Any) -> ModelMonitor:
    """Create configured model monitor."""
    return ModelMonitor(**kwargs)


def create_alert_manager(**kwargs: Any) -> AlertManager:
    """Create configured alert manager."""
    return AlertManager(**kwargs)
