import io
import os

import googleapiclient.discovery
import googleapiclient.errors
import googleapiclient.http
import numpy as np

from google_drive_utils.google_drive_credentials import \
    get_google_drive_credentials


def get_google_drive_file_id(gdrive_service, dir_id):
    """Получает идентификатор файла Google Drive."""
    page_token = None
    files = []
    query = f"mimeType = 'image/tiff' and '{dir_id}' in parents"
    try:
        while True:
            response = (
                gdrive_service.files()
                .list(q=query, spaces="drive", fields="nextPageToken, files(id, name)", pageToken=page_token)
                .execute()
            )
            files.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred: {error}")
        files = None
    return files


def get_google_drive_dir_id(gdrive_service, dir_name):
    """Получает идентификатор каталога Google Drive."""
    page_token = None
    folders = []
    query = f"name = '{dir_name}' and mimeType = 'application/vnd.google-apps.folder'"
    try:
        while True:
            response = (
                gdrive_service.files()
                .list(q=query, spaces="drive", fields="nextPageToken, files(id, name)", pageToken=page_token)
                .execute()
            )
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred: {error}")
        folders = None
    return folders


def download_google_drive_id(gdrive_service, file_id):
    """Загружает файл с Google Drive по его идентификатору."""
    try:
        request = gdrive_service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = googleapiclient.http.MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred: {error}")
        file = None
        return 0
    return file.getvalue()


def download_img_google_drive(filename, output_folder, token_json, credentials_json, SCOPES):
    """Загружает изображение с Google Drive."""
    creds = get_google_drive_credentials(token_json, credentials_json, [SCOPES])
    service = googleapiclient.discovery.build("drive", "v3", credentials=creds)
    folder_list = get_google_drive_dir_id(service, output_folder)
    for folder in folder_list:
        folder_id = folder["id"]
        folder_name = folder["name"]
        if folder_name != output_folder:
            continue
        file_list = get_google_drive_file_id(service, folder_id)
        idx_select = np.argmax([f["name"] == filename for f in file_list])
        if file_list and file_list[idx_select]["name"] == filename:
            file_id = file_list[idx_select]["id"]
            download_code = download_google_drive_id(service, file_id)
            if download_code:
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)
                with open(f"{output_folder}/{filename}", "wb") as f:
                    f.write(download_code)
                return 0
    return None
