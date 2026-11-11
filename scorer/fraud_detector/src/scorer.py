import os
import pickle
import logging
import pandas as pd
import time

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/current_model.pkl")
VERSION_PATH = MODEL_PATH + ".version" 
FRAUD_THRESHOLD = 0.5

_model = None
_model_mtime = 0.0
_model_version = "unknown"

def _load_model():
    global _model, _model_mtime, _model_version

    if not os.path.exists(MODEL_PATH):
        legacy = "/app/models/logreg_model.pkl"
        if os.path.exists(legacy) and _model is None:
            logger.warning("Model not found at %s, falling back to legacy %s", MODEL_PATH, legacy)
            with open(legacy, "rb") as f:
                _model = pickle.load(f)
            _model_version = "legacy"
        return

    mtime = os.path.getmtime(MODEL_PATH)

    if mtime > _model_mtime:
        with open(MODEL_PATH, "rb") as f:
            _model = pickle.load(f)

        if os.path.exists(VERSION_PATH):
            with open(VERSION_PATH) as f:
                _model_version = f.read().strip()
        else:
            _model_version = f"mtime_{int(mtime)}"

        _model_mtime = mtime
        logger.info("Model loaded successfully. Version: %s", _model_version)

def make_pred(dt: pd.DataFrame, source_info: str = "kafka") -> pd.DataFrame:
    logger.info("Making prediction for data from %s, shape: %s", source_info, dt.shape)

    _load_model()
    if _model is None:
        raise ValueError("Model is not loaded yet!")

    proba = _model.predict_proba(dt)[:, 1]

    submission = pd.DataFrame({
        "score": proba,
        "fraud_flag": (proba > FRAUD_THRESHOLD).astype(int),
        "model_version": _model_version
    })

    logger.info("Prediction complete for data from %s. Model version: %s", source_info, _model_version)
    return submission