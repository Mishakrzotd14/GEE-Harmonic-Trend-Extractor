import os
import time

import ee

from config_utils import config

ee.Initialize(project=config.gee_project_name)
aoi = ee.FeatureCollection(config.aoi_name)
CLOUD_PROB_THRESHOLD = config.cloud_threshold


def filter_bounds_date(img_col):
    """Фильтрует коллекцию изображений по границам и диапазону дат."""
    return (
        img_col.filterBounds(aoi)
        .filter(ee.Filter.calendarRange(2017, 2023, "year"))
        .filter(ee.Filter.calendarRange(4, 11, "month"))
    )


def index_join(col_a, col_b, prop_name):
    """Выполняет соединение по индексу двух коллекций изображений."""
    joined = ee.ImageCollection(
        ee.Join.saveFirst(prop_name).apply(
            primary=col_a,
            secondary=col_b,
            condition=ee.Filter.equals(leftField="system:index", rightField="system:index"),
        )
    )
    return joined.map(lambda image: image.addBands(ee.Image(image.get(prop_name))))


def build_mask_function(cloud_prob):
    """Создает функцию маскировки облачности."""

    def mask_clouds(img):
        cloud = img.select("probability").gt(cloud_prob)
        return img.updateMask(cloud.Not())

    return mask_clouds


def add_variables(image):
    """Добавляет дополнительные переменные к каждому изображению."""
    date = ee.Date(image.date())
    years = date.difference(ee.Date("1970-01-01"), "year")
    return (
        image.addBands(image.add(1000).normalizedDifference(["B8", "B4"]).rename("ndvi"))
        .addBands(ee.Image(years).rename("t").toFloat())
        .addBands(ee.Image.constant(1))
    )


def compute_harmonic_trends(image):
    """Вычисляет гармонические тренды для каждого изображения."""
    time_radians = image.select("t").toFloat().multiply(2 * 3.141592653589793)
    return image.addBands(time_radians.cos().rename("cos")).addBands(time_radians.sin().rename("sin"))


def compute_harmonic_trend_coefficients():
    """Вычисляет коэффициенты гармонического тренда."""
    mask = ee.Image.constant(1).clip(aoi.geometry()).mask()

    s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterMetadata(
        "CLOUD_COVERAGE_ASSESSMENT", "less_than", CLOUD_PROB_THRESHOLD
    )
    s2_cloud = ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")

    s2 = filter_bounds_date(s2, aoi).map(lambda image: image.updateMask(mask)).select(["B8", "B4"])
    s2_cloud = filter_bounds_date(s2_cloud, aoi).map(lambda image: image.updateMask(mask))

    with_cloud_probability = index_join(s2, s2_cloud, "cloud_probability")
    mask_clouds = build_mask_function(CLOUD_PROB_THRESHOLD)
    s2_masked = (
        ee.ImageCollection(with_cloud_probability.map(mask_clouds)).map(add_variables).select(["ndvi", "t", "constant"])
    )

    harmonic_independents = ee.List(["constant", "t", "cos", "sin"])
    harmonic_sent = s2_masked.map(compute_harmonic_trends)
    variables_for_regression = harmonic_independents.add("ndvi")
    harmonic_trend = harmonic_sent.select(variables_for_regression).reduce(
        ee.Reducer.robustLinearRegression(numX=harmonic_independents.length(), numY=1)
    )
    harmonic_trend_coefficients = (
        harmonic_trend.select("coefficients").arrayProject([0]).arrayFlatten([harmonic_independents])
    )

    return harmonic_trend_coefficients


def export_to_drive(filename, loc_name, max_pixels=1e8):
    """Экспортирует изображение в Google Drive."""
    basename = os.path.splitext(filename)[0]
    image = compute_harmonic_trend_coefficients()
    export_task = ee.batch.Export.image.toDrive(
        image=image,
        description=basename,
        scale=10,
        region=aoi.geometry(),
        fileNamePrefix=basename,
        crs="EPSG:32635",
        fileFormat="GeoTIFF",
        folder=f"{loc_name}",
        maxPixels=max_pixels,
    )
    export_task.start()
    waiting = True
    while waiting:
        if export_task.status()["state"] in ["COMPLETED", "FAILED"]:
            waiting = False
        else:
            time.sleep(5)
    return 0
