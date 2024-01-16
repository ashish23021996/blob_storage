import abc
import time
from datetime import datetime, timedelta

from app import settings
from app.blob_storage.enum import StorageResponseTypeEnum
from app.core.aes_encryption import AESEncryption


class BlobStorage:
    storages = dict()
    DOMAIN = f"{settings.DOMAIN}/api/v1/media/"

    @property
    @abc.abstractmethod
    def type(self):
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.storages[cls.type] = cls

    def get_expiry_in_epoch(self):
        # weeks 260 = almost 5 years
        return round((datetime.now() + timedelta(weeks=260)).timestamp())

    @abc.abstractmethod
    async def get_object(self, url: str):
        pass

    @abc.abstractmethod
    async def store_object_from_url(self, url, bot_id, response_type, file_name=None):
        pass

    async def create_object_path(self, response_type: StorageResponseTypeEnum, bot_id, file_name):
        current_month = datetime.now().month
        current_year = datetime.now().year
        return f"{response_type.value}/{bot_id}/{current_year}/{current_month}/{file_name}"

    async def update_filename_with_time(self, file_name: str):
        return str(round(datetime.now().timestamp())) + f"_{file_name}"

    async def get_info_from_object_path(self, path: str):
        response_type, bot_id, current_year, current_month, file_name = path.split("/")
        return bot_id, file_name

    async def create_presigned_url(self, base_path, extension=None):
        # TODO aes logic
        expiry = self.get_expiry_in_epoch()

        url = f"{base_path}::{expiry}"

        encrypted_url = await BlobStorage.encrypt_path(url)

        if extension:
            encrypted_url = encrypted_url + f".{extension}"

        presigned_url = self.DOMAIN + encrypted_url
        return presigned_url

    @classmethod
    async def encrypt_path(cls, path):
        """
        # This function encrypts the path passed to it as a function parameter

        :param message: message to be encryted
        :return: encrypted path
        """
        encoded = await AESEncryption().encrypt(path)
        # TODO Need to better replace logic. Added because if / exists it updates api path
        return encoded.decode('UTF-8').replace("/", "@*!")

    @classmethod
    async def decrypt_path(cls, encrypted_path):
        # TODO Need to better replace logic. Added because if / exists it updates api path
        encrypted_path = encrypted_path.replace("@*!", "/")
        return await AESEncryption().decrypt(encrypted_path)

    @classmethod
    async def get_file_from_presigned_url(cls, url_with_extension: str):

        extension = None
        url_with_extension = url_with_extension.split(".")
        if len(url_with_extension) > 1:
            # if needed we can pass this
            extension = url_with_extension[-1]

        presigned_path = url_with_extension[0]

        decrypted_base_path = await cls.decrypt_path(presigned_path)

        base_path, expiry = decrypted_base_path.split("::")

        if time.time() > int(expiry):
            raise ValueError("File already expired")

        return await cls.storages[settings.BLOB_STORAGE_TYPE]().get_object(base_path)
