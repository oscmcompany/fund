from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
INFRASTRUCTURE_STORAGE_PATH = REPOSITORY_ROOT / "infrastructure" / "storage.py"


def load_infrastructure_storage() -> str:
    return INFRASTRUCTURE_STORAGE_PATH.read_text(encoding="utf-8")


def test_storage_contains_s3_bucket_encryption_resources() -> None:
    infrastructure_storage = load_infrastructure_storage()

    assert '"data_bucket_encryption"' in infrastructure_storage
    assert '"model_artifacts_bucket_encryption"' in infrastructure_storage


def test_storage_contains_s3_public_access_block_resources() -> None:
    infrastructure_storage = load_infrastructure_storage()

    assert '"data_bucket_public_access_block"' in infrastructure_storage
    assert '"model_artifacts_bucket_public_access_block"' in infrastructure_storage
