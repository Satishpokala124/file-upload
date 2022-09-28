import os
import time
from datetime import datetime
from functools import wraps

import boto3
import magic
import uvicorn
from fastapi import (BackgroundTasks, FastAPI, File, HTTPException, UploadFile,
                     status)

KB = 1024
MB = 1024 * KB
PWD = os.getcwd()
MEDIA_FOLDER = f'{PWD}/media'
IMAGE_FOLDER = f'{MEDIA_FOLDER}/images/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}'
PDF_FOLDER = f'{MEDIA_FOLDER}/documents/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}'
SUPPORTED_FILE_TYPES = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'application/pdf': 'pdf'
}
AWS_BUCKET = 'satish-file-upload'


def create_folder(folder):
    if not os.path.isdir(folder):
        os.makedirs(folder)


s3 = boto3.resource('s3')
bucket = s3.Bucket(AWS_BUCKET)


app = FastAPI()


def timer(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        resp = await func(*args, **kwargs)
        print(f'{func.__name__} -> {time.time() - start_time}')
        return resp
    return wrapper


@timer
async def s3_upload(file_path: str, prefix: str, file_name: str):
    with open(file_path, 'rb') as data:
        bucket.put_object(Key=f'{prefix}/{file_name}', Body=data)


@app.post('/files/')
@timer
async def create_file(file: bytes | None = File(default=None)):
    print(file)
    return {'file_size': len(file)}


@app.post('/uploadfile/')
@timer
async def create_upload_file(background_tasks: BackgroundTasks, file: UploadFile | None = None):
    contents = await file.read()
    file_size = len(contents)
    if not 0 < file_size < 1*MB:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail='Supported file size is less than 1 MB'
        )
    file_type = magic.from_buffer(contents, mime=True)
    if file_type.lower() not in SUPPORTED_FILE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Unsupported file type {file_type}. Supported types are {SUPPORTED_FILE_TYPES}'
        )
    folder = PDF_FOLDER if file_type == 'application/pdf' else IMAGE_FOLDER
    create_folder(folder)
    file_name = f'{datetime.now().strftime("%H:%M:%S:%f")}.{SUPPORTED_FILE_TYPES[file_type]}'
    with open(f'{folder}/{file_name}', 'wb') as new_file:
        new_file.write(contents)
        print(new_file.name)
        background_tasks.add_task(
            s3_upload, new_file.name, 'file-upload', file_name)
    return {'file_size': file_size}


if __name__ == "__main__":
    uvicorn.run(app='main:app', reload=True)
