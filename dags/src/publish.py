import os
import json
import shutil
import logging
import datetime

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
MODELS_DIR = os.getenv("MODELS_DIR", "/opt/airflow/models")


def _build_version():
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"v_{stamp}"


def publish_model(logical_month):
    version = _build_version()

    model_src = os.path.join(DATA_DIR, "model_candidate.pkl")
    model_dst = os.path.join(MODELS_DIR, "current_model.pkl")
    model_tmp = model_dst + ".tmp"

    if not os.path.exists(model_src):
        raise FileNotFoundError(f"Candidate model not found: {model_src}")

    shutil.copy(model_src, model_tmp)
    os.replace(model_tmp, model_dst)
    logger.info("Model published: %s", model_dst)

    version_path = model_dst + ".version"
    with open(version_path, "w") as f:
        f.write(version)

    metrics_src = os.path.join(DATA_DIR, "model_candidate.metrics.json")
    metrics_dst = os.path.join(MODELS_DIR, "current_model.metrics.json")
    shutil.copy(metrics_src, metrics_dst)

    state_path = os.path.join(DATA_DIR, "state.json")
    with open(state_path, "w") as f:
        json.dump({"last_month": logical_month}, f)
    logger.info("State updated: last_month=%s", logical_month)

    return {"version": version, "month": logical_month}