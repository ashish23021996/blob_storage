import io
import os

from app import settings
from app.blob_storage.base_storage import BlobStorage
from app.blob_storage.enum import StorageResponseTypeEnum, BlobStorageType
from app.core.requests import Request
from app.logger import logger
from app.core.utils import get_file_name_from_url


class NFSStorage(BlobStorage):
    type = BlobStorageType.NFS.value

    def __init__(self):
        self.base_path = settings.NFS_PATH

    def get_nfs_path(self, file_path: str):
        return os.path.join(self.base_path, file_path)

    async def get_object(self, file_path: str):
        _, file_name = await super().get_info_from_object_path(file_path)
        nfs_path = self.get_nfs_path(file_path)
        return nfs_path, file_name

    async def store_object_from_url(self, url: str, bot_id: str, response_type: StorageResponseTypeEnum,
                                    file_name: str = None):
        status, response_data, response_content = await Request().http_get(url)
        if status != 200:
            logger.error(f'Error downloading file from presigned URL :: {url}')
            raise ValueError(f"Unable to download media from url {url}")

        if not file_name:
            file_name = await get_file_name_from_url(url)

        file_name = await super().update_filename_with_time(file_name)

        obj_key = await super().create_object_path(response_type=response_type, bot_id=bot_id, file_name=file_name)

        nfs_path = self.get_nfs_path(obj_key)

        # Upload the file to S3
        self.put_object(content=io.BytesIO(response_data), path=nfs_path)

        logger.info(f"File uploaded successfully to nfs path: {nfs_path}")
        file_name_with_extension = file_name.split(".")
        extension = None
        if len(file_name_with_extension) > 1:
            extension = file_name_with_extension[-1]

        return await super().create_presigned_url(base_path=obj_key, extension=extension)

    def put_object(self, content: io.BytesIO, path: str):
        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with open(path, 'wb') as file:
                file.write(content.read())
            logger.info(f"File written successfully to path {path}")
        except IOError:
            logger.error(f"An error occurred while writing the file to path {path}")
