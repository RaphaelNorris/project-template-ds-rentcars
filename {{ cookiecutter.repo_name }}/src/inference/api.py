"""
FastAPI Model Serving API for MLOps Pipeline

This module provides REST endpoints for:
- Model inference
- Health checks
- Model metadata
- Predictions logging
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Union

import mlflow
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from prometheus_client import Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from starlette.responses import Response

from src.model.mlflow_manager import MLFlowManager
from src.inference.monitoring import ModelMonitor


# Prometheus metrics
REQUEST_COUNT = Counter(
    "model_predictions_total",
    "Total number of predictions",
    ["model_name", "model_version"],
)
REQUEST_LATENCY = Histogram(
    "model_prediction_latency_seconds",
    "Prediction latency in seconds",
    ["model_name", "model_version"],
)
PREDICTION_ERRORS = Counter(
    "model_prediction_errors_total",
    "Total number of prediction errors",
    ["model_name", "model_version", "error_type"],
)


# Global variables for model
MODEL = None
MODEL_METADATA = {}
MLFLOW_MANAGER = None
MONITOR = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    logger.info("Starting up model serving API...")

    global MODEL, MODEL_METADATA, MLFLOW_MANAGER, MONITOR

    try:
        # Initialize MLFlow manager
        MLFLOW_MANAGER = MLFlowManager()

        # Load model from MLFlow
        model_name = os.getenv("MODEL_NAME", "champion_model")
        model_stage = os.getenv("MODEL_STAGE", "Production")

        logger.info(f"Loading model: {model_name} ({model_stage})")

        model_version = MLFLOW_MANAGER.get_latest_model_version(
            name=model_name,
            stage=model_stage,
        )

        if model_version is None:
            logger.warning(f"No model found with name {model_name} in {model_stage}")
        else:
            model_uri = f"models:/{model_name}/{model_stage}"
            MODEL = mlflow.pyfunc.load_model(model_uri)

            MODEL_METADATA = {
                "name": model_name,
                "version": model_version.version,
                "stage": model_stage,
                "run_id": model_version.run_id,
            }

            logger.info(f"Model loaded successfully: {MODEL_METADATA}")

        # Initialize monitor
        MONITOR = ModelMonitor()

    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        MODEL_METADATA["error"] = str(e)

    yield

    # Shutdown
    logger.info("Shutting down model serving API...")


# Create FastAPI app
app = FastAPI(
    title="ML Model Serving API",
    description="REST API for ML model inference",
    version="1.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class PredictionRequest(BaseModel):
    """Request model for predictions."""

    features: Union[List[Dict[str, Any]], Dict[str, Any]] = Field(
        ...,
        description="Features for prediction (single dict or list of dicts)",
    )
    model_name: Optional[str] = Field(
        None,
        description="Model name (optional, uses default if not provided)",
    )
    model_version: Optional[str] = Field(
        None,
        description="Model version (optional, uses latest if not provided)",
    )


class PredictionResponse(BaseModel):
    """Response model for predictions."""

    predictions: List[Any] = Field(..., description="Model predictions")
    model_metadata: Dict[str, Any] = Field(..., description="Model metadata")
    inference_time_ms: float = Field(..., description="Inference time in milliseconds")


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str = Field(..., description="Service status")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    model_metadata: Dict[str, Any] = Field(..., description="Model metadata")


# API endpoints
@app.get("/", tags=["General"])
async def root():
    """Root endpoint."""
    return {
        "message": "ML Model Serving API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy" if MODEL is not None else "degraded",
        model_loaded=MODEL is not None,
        model_metadata=MODEL_METADATA,
    )


@app.get("/metrics", tags=["Monitoring"])
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type="text/plain")


@app.post("/predict", response_model=PredictionResponse, tags=["Inference"])
async def predict(request: PredictionRequest, http_request: Request):
    """
    Make predictions using the loaded model.

    Args:
        request: Prediction request
        http_request: HTTP request object

    Returns:
        Prediction response with results and metadata
    """
    if MODEL is None:
        PREDICTION_ERRORS.labels(
            model_name=MODEL_METADATA.get("name", "unknown"),
            model_version=MODEL_METADATA.get("version", "unknown"),
            error_type="model_not_loaded",
        ).inc()
        raise HTTPException(status_code=503, detail="Model not loaded")

    start_time = time.time()

    try:
        # Convert features to DataFrame
        if isinstance(request.features, dict):
            features_list = [request.features]
        else:
            features_list = request.features

        df = pd.DataFrame(features_list)

        # Make predictions
        predictions = MODEL.predict(df)

        # Convert predictions to list
        if hasattr(predictions, "tolist"):
            predictions_list = predictions.tolist()
        else:
            predictions_list = list(predictions)

        # Calculate inference time
        inference_time_ms = (time.time() - start_time) * 1000

        # Update metrics
        REQUEST_COUNT.labels(
            model_name=MODEL_METADATA.get("name", "unknown"),
            model_version=MODEL_METADATA.get("version", "unknown"),
        ).inc()
        REQUEST_LATENCY.labels(
            model_name=MODEL_METADATA.get("name", "unknown"),
            model_version=MODEL_METADATA.get("version", "unknown"),
        ).observe(inference_time_ms / 1000)

        # Log prediction for monitoring
        if MONITOR:
            MONITOR.log_prediction(
                features=df,
                predictions=predictions_list,
                model_version=MODEL_METADATA.get("version"),
            )

        logger.info(
            f"Prediction completed: {len(predictions_list)} samples, "
            f"{inference_time_ms:.2f}ms",
        )

        return PredictionResponse(
            predictions=predictions_list,
            model_metadata=MODEL_METADATA,
            inference_time_ms=inference_time_ms,
        )

    except Exception as e:
        PREDICTION_ERRORS.labels(
            model_name=MODEL_METADATA.get("name", "unknown"),
            model_version=MODEL_METADATA.get("version", "unknown"),
            error_type=type(e).__name__,
        ).inc()
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/predict/batch", tags=["Inference"])
async def predict_batch(features: List[Dict[str, Any]]):
    """
    Make batch predictions.

    Args:
        features: List of feature dictionaries

    Returns:
        List of predictions
    """
    request = PredictionRequest(features=features)
    return await predict(request, None)


@app.get("/model/info", tags=["Model"])
async def model_info():
    """Get model information."""
    if MODEL is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return MODEL_METADATA


@app.post("/model/reload", tags=["Model"])
async def reload_model():
    """Reload the model from MLFlow."""
    global MODEL, MODEL_METADATA

    try:
        model_name = os.getenv("MODEL_NAME", "champion_model")
        model_stage = os.getenv("MODEL_STAGE", "Production")

        model_version = MLFLOW_MANAGER.get_latest_model_version(
            name=model_name,
            stage=model_stage,
        )

        if model_version is None:
            raise HTTPException(
                status_code=404,
                detail=f"No model found: {model_name} ({model_stage})",
            )

        model_uri = f"models:/{model_name}/{model_stage}"
        MODEL = mlflow.pyfunc.load_model(model_uri)

        MODEL_METADATA = {
            "name": model_name,
            "version": model_version.version,
            "stage": model_stage,
            "run_id": model_version.run_id,
        }

        logger.info(f"Model reloaded: {MODEL_METADATA}")

        return {"status": "success", "model_metadata": MODEL_METADATA}

    except Exception as e:
        logger.error(f"Failed to reload model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("MODEL_SERVING_HOST", "0.0.0.0"),
        port=int(os.getenv("MODEL_SERVING_PORT", 8000)),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
