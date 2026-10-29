import json

import joblib
import numpy as np
import pytest

from fraud_detection.inference import (
    FEATURE_CONTRACT_VERSION,
    FEATURES,
    file_sha256,
    load_model_bundle,
)


class TinyModel:
    def predict_proba(self, frame):
        return np.tile(np.array([[0.7, 0.3]], dtype=float), (len(frame), 1))


def test_model_bundle_validates_artifact_checksum(tmp_path):
    model_path = tmp_path / "model.joblib"
    metadata_path = tmp_path / "metadata.json"
    joblib.dump(TinyModel(), model_path)

    metadata = {
        "model_version": "tiny-v1",
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "features": FEATURES,
        "review_threshold": 0.5,
        "artifact_sha256": file_sha256(model_path),
    }
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    bundle = load_model_bundle(model_path, metadata_path)
    assert bundle.model_version == "tiny-v1"

    metadata["artifact_sha256"] = "0" * 64
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        load_model_bundle(model_path, metadata_path)
