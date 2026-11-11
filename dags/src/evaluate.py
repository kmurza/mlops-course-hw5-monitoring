import os
import json
import logging

logger = logging.getLogger(__name__)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
MODELS_DIR = os.getenv("MODELS_DIR", "/opt/airflow/models")
METRIC_KEY = "roc_auc"


def _read_metric(path):
    with open(path) as f:
        payload = json.load(f)

    if METRIC_KEY not in payload:
        raise ValueError(f"Key '{METRIC_KEY}' is missing in {path}")

    return float(payload[METRIC_KEY])


def is_candidate_better():
    candidate_path = os.path.join(DATA_DIR, "model_candidate.metrics.json")
    if not os.path.exists(candidate_path):
        raise FileNotFoundError(f"Candidate metrics not found: {candidate_path}")

    candidate_auc = _read_metric(candidate_path)

    prod_path = os.path.join(MODELS_DIR, "current_model.metrics.json")
    if not os.path.exists(prod_path):
        logger.info(
            "No prod model yet, publishing candidate (roc_auc=%.4f)",
            candidate_auc,
        )
        return True

    prod_auc = _read_metric(prod_path)
    logger.info(
        "Candidate roc_auc=%.4f vs prod roc_auc=%.4f",
        candidate_auc, prod_auc,
    )

    return candidate_auc >= prod_auc