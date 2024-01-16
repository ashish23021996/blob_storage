from enum import Enum

from app.core.enum import EnumMixin


class StorageResponseTypeEnum(EnumMixin, Enum):
    ANALYTICS_REPORTS = "analytics_reports"
    USER_RESPONSES = "user_responses"


class BlobStorageType(EnumMixin, Enum):
    S3 = "s3"
    NFS = "nfs"
