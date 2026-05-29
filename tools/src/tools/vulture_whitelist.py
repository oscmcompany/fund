# Vulture whitelist - mark intentionally unused code
# These symbols are imported unconditionally so vulture recognizes them as used

from mypy_boto3_s3 import S3Client
from tenacity import RetryCallState

_ = S3Client  # used in type annotations
_ = RetryCallState  # used in string annotation for type checker
datamanager_base_url  # shim param retained until Phase 9 cleanup
