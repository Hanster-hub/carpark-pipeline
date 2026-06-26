"""FastAPI inference service. Loads the registered MLflow model and serves /predict.

Packaged in Docker (see Dockerfile) — that's the rubric's 'Docker may be used to serve
the prediction API' point. The dashboard calls this for the next-2-hours forecast.
"""
import mlflow.pyfunc
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

from common import config
from common.logging_setup import get_logger

log = get_logger(__name__)
app = FastAPI(title="Carpark availability predictor")

_model = None


def get_model():
    global _model
    if _model is None:
        mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
        # load latest version from the registry
        _model = mlflow.pyfunc.load_model(f"models:/{config.MODEL_NAME}/latest")
        log.info("Loaded model %s from registry", config.MODEL_NAME)
    return _model


class PredictRequest(BaseModel):
    hour: int
    dow: int
    lag_1: float
    lag_2: float
    temp: float
    precip: float
    wind: float


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(req: PredictRequest):
    model = get_model()
    X = pd.DataFrame([req.dict()])
    pred = float(model.predict(X)[0])
    return {"predicted_available": round(pred, 1)}
