import os
import pickle
import logging
import pandas as pd
from prometheus_client import Counter, Gauge

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/current_model.pkl")
VERSION_PATH = MODEL_PATH + ".version"
FRAUD_THRESHOLD = 0.5

_model = None
_model_mtime = 0.0
_model_version = "unknown"

model_reload_total = Counter("scoring_model_reload_total", "Model reloads", ["version"])
model_info = Gauge("scoring_model_info", "Current model version", ["version"])

def _load_model():
    global _model, _model_mtime, _model_version

    if not os.path.exists(MODEL_PATH):
        legacy = "/app/models/logreg_model.pkl"
        if os.path.exists(legacy) and _model is None:
            with open(legacy, "rb") as f:
                _model = pickle.load(f)
            _model_version = "legacy"
            model_info.labels(version=_model_version).set(1)
        return

    mtime = os.path.getmtime(MODEL_PATH)

    if mtime > _model_mtime:
        with open(MODEL_PATH, "rb") as f:
            _model = pickle.load(f)

        if os.path.exists(VERSION_PATH):
            with open(VERSION_PATH) as f:
                new_version = f.read().strip()
        else:
            new_version = f"mtime_{int(mtime)}"

        if new_version != _model_version:
            model_reload_total.labels(version=new_version).inc()
            try:
                model_info.clear()
            except Exception:
                pass
            model_info.labels(version=new_version).set(1)

        _model_version = new_version
        _model_mtime = mtime
        logger.info("Model loaded successfully. Version: %s", _model_version)


def make_pred(dt: pd.DataFrame, source_info: str = "kafka") -> pd.DataFrame:
    _load_model()
    if _model is None:
        raise ValueError("Model is not loaded yet!")

    proba = _model.predict_proba(dt)[:, 1]

    submission = pd.DataFrame({
        "score": proba,
        "fraud_flag": (proba > FRAUD_THRESHOLD).astype(int),
        "model_version": _model_version,
    })

    return submission