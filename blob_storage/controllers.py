
from fastapi import APIRouter, Request, Depends
from starlette.responses import FileResponse, RedirectResponse

from app import settings
from app.blob_storage.base_storage import BlobStorage

from app.blob_storage.enum import BlobStorageType, StorageResponseTypeEnum
from app.core import verify_surbo_chat_token

router = APIRouter(prefix="/v1/media")

@router.get("/get_upload_url", dependencies=[Depends(verify_surbo_chat_token)], )
async def get_upload_url(identifier: str, response_type: StorageResponseTypeEnum = StorageResponseTypeEnum.USER_RESPONSES,
                         file_name: str = None):
    upload_url, download_url = await BlobStorage.storages[
        settings.BLOB_STORAGE_TYPE]().generate_presigned_url_for_upload(identifier,
                                                                        response_type,
                                                                        file_name)

    return {"upload_url": upload_url, "download_url": download_url}


@router.put("/{url}")
async def upload_media(url: str, request: Request):
    body = await request.body()
    await BlobStorage.storages[
        BlobStorageType.NFS.value]().upload_file(url, response_data=body)


@router.get("/{url}")
async def get_file(url: str):
    content_path, file_name = await BlobStorage.get_file_from_presigned_url(url)
    headers = {
        f'Content-Disposition': f'attachment; filename={file_name}'
    }

    if settings.BLOB_STORAGE_TYPE == BlobStorageType.NFS.value:
        return FileResponse(content_path, headers=headers)

    return RedirectResponse(content_path)
