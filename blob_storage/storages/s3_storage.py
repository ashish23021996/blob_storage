import io

import boto3
import magic
from botocore.config import Config

from app import settings
from app.blob_storage.base_storage import BlobStorage
from app.blob_storage.enum import StorageResponseTypeEnum, BlobStorageType
from app.core.requests import Request
from app.logger import logger
from app.core.utils import get_file_name_from_url


class S3Storage(BlobStorage):
    type = BlobStorageType.S3.value

    def __init__(self):
        self.s3 = boto3.client('s3', aws_access_key_id=settings.AWS_ACCESS_KEY,
                               aws_secret_access_key=settings.AWS_ACCESS_SECRET,
                               config=Config(signature_version='s3v4'), region_name=settings.AWS_REGION)
        self.bucket = settings.AWS_BUCKET

    async def get_object_from_s3(self, file_path: str):
        s3_response_object = self.s3.get_object(Bucket=self.bucket, Key=file_path)
        object_content = s3_response_object['Body']
        return object_content

    async def get_object(self, file_path: str):
        _, file_name = await super().get_info_from_object_path(file_path)
        presigned_url = await self.get_presigned_url_from_s3(file_path)
        return presigned_url, file_name

    async def store_object_from_url(self, url: str, bot_id: str, response_type: StorageResponseTypeEnum,
                                    file_name: str = None, **kwargs):
        status, response_data, response_content = await Request().http_get(url)
        if status != 200:
            logger.error(f'Error downloading file from presigned URL :: {url}')
            raise ValueError(f"Unable to download media from url {url}")

        if not file_name:
            file_name = await get_file_name_from_url(url)

        file_name = await super().update_filename_with_time(file_name)

        obj_key = await super().create_object_path(response_type=response_type, bot_id=bot_id, file_name=file_name)

        # Upload the file to S3
        if kwargs.get('content_type'):
            mime_type = magic.from_buffer(response_data, mime=True)
            self.s3.put_object(Body=io.BytesIO(response_data), Bucket=self.bucket, Key=obj_key, ContentType=mime_type)
        else:
            self.s3.put_object(Body=io.BytesIO(response_data), Bucket=self.bucket, Key=obj_key)

        logger.info(f"File uploaded successfully to S3 bucket: {self.bucket}, object key: {obj_key}")
        file_name_with_extension = file_name.split(".")
        extension = None
        if len(file_name_with_extension) > 1:
            extension = file_name_with_extension[-1]

        return await super().create_presigned_url(base_path=obj_key, extension=extension)

    async def get_presigned_url_from_s3(self, url_path: str):
        # AWS presigned url can only last for max 7 days
        url = self.s3.generate_presigned_url('get_object', Params={'Bucket': self.bucket, 'Key': url_path},
                                             ExpiresIn=settings.AWS_PRESIGNED_URL_EXPIRY)
        return url

    async def store_object_from_content(self, media_bytes: bytes, bot_id: str, response_type: StorageResponseTypeEnum,
                                        file_name: str = None):

        obj_key = await super().create_object_path(response_type=response_type, bot_id=bot_id, file_name=file_name)

        # Upload the file to S3
        self.s3.put_object(Body=io.BytesIO(media_bytes), Bucket=self.bucket, Key=obj_key)

        logger.info(f"File uploaded successfully to S3 bucket: {self.bucket}, object key: {obj_key}")
        file_name_with_extension = file_name.split(".")
        extension = None
        if len(file_name_with_extension) > 1:
            extension = file_name_with_extension[-1]

        return await super().create_presigned_url(base_path=obj_key, extension=extension)
