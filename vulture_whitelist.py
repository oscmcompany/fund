# Vulture whitelist - mark intentionally unused code
# These imports are used only for type hints under TYPE_CHECKING

from mypy_boto3_s3 import S3Client

_ = S3Client  # used in type annotations
