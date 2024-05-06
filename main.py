import datetime
import sys

from config_utils import config
from gee_image_analysis.harmonic_trend_analysis import export_to_drive
from google_drive_utils.google_drive_operations import \
    download_img_google_drive


def process_harmonic_trend(filename, folder_download):
    """Получает гармонический тренд, экспортирует в Google Drive и загружает его локально на компьютер."""
    time_start_export = datetime.datetime.now()
    export_code = export_to_drive(filename, folder_download)

    if export_code is None:
        print("Экспорт на Google Drive не удался.")
        print("Попытка с большим количеством пикселей.")
        export_code = export_to_drive(filename, folder_download, max_pixels=1e9)
        if export_code is None:
            print("Экспорт на Google Drive не удался.")
            sys.exit()

    time_end_export = datetime.datetime.now()
    export_duration = time_end_export - time_start_export
    print(f"Обработка Sentinel-2 заняла {export_duration.seconds + export_duration.microseconds/1e6:.1f} с.")

    time_start_download = datetime.datetime.now()
    download_code = download_img_google_drive(
        filename, folder_download, config.token_json, config.credentials_json, config.scopes
    )

    if download_code is None:
        print("Не удалось загрузить изображение с Google Drive.")
        sys.exit()

    time_end_download = datetime.datetime.now()
    download_duration = time_start_download - time_end_download
    print(f"Обработка изображения заняла {download_duration.seconds + download_duration.microseconds/1e6:.1f} с.")


if __name__ == "__main__":
    process_harmonic_trend(config.raster_name, config.download_raster_path)
