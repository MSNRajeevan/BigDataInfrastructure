from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from google.cloud import storage
import datetime

# Initialize SparkSession with BigQuery support.
spark = SparkSession.builder \
    .appName('WeatherAPIDataToBigQuery') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0') \
    .getOrCreate()

current_date = datetime.date.today().isoformat()

json_path = f'gs://bigdatadump-apitobq/weatherapi/{current_date}/*'

storage_client = storage.Client()

archive_config = {
    "source_bucket_name": "bigdatadump-apitobq",
    "source_folder_name": f"weatherapi/{current_date}",
    "destination_bucket_name": "weather_api_data_archives",
    "destination_folder_name": f"weather_archive/{current_date}"
}

source_bucket = storage_client.bucket(archive_config["source_bucket_name"])
destination_bucket = storage_client.bucket(archive_config["destination_bucket_name"])

blobs = list(source_bucket.list_blobs(prefix=archive_config["source_folder_name"]))

if not blobs:
    print("No data found in the specified folder.")
else:
    weather_df = spark.read.json(json_path)

    # Flatten the DataFrame by selecting and renaming nested fields.
    flattened_df = weather_df.select(
        explode(col("data")).alias("data")
    ).select(
        col("data.location.name").alias("location_name"),
        col("data.location.country").alias("country"),
        col("data.location.lat").alias("latitude"),
        col("data.location.lon").alias("longitude"),
        col("data.location.timezone_id").alias("timezone"),
        col("data.current.observation_time").alias("observation_time"),
        col("data.current.temperature").alias("temperature_degree_celcius"),
        col("data.current.weather_descriptions")[0].alias("weather_description"),
        col("data.current.wind_speed").alias("wind_speed_km_per_hr"),
        col("data.current.wind_dir").alias("wind_direction"),
        col("data.current.pressure").alias("pressure_Pa"),
        col("data.current.precip").alias("precipitation_mm"),
        col("data.current.humidity").alias("humidity_g_per_m3"),
        col("data.current.cloudcover").alias("cloud_cover_oktas"),
        col("data.current.feelslike").alias("feels_like_degree_celcius"),
        col("data.current.uv_index").alias("uv_index_degree_celcius"),
        col("data.current.visibility").alias("visibility_m"),
        col("data.current.is_day").alias("is_day")
    )

    # Show the DataFrame to verify the results.
    flattened_df.show(truncate=False)

    # Write the flattened DataFrame to BigQuery.
    flattened_df.write.format('bigquery') \
        .option('table', 'scenic-style-420014.BigData767.weatherTable') \
        .option('temporaryGcsBucket', 'weatherapidatatmp') \
        .mode('overwrite') \
        .save()

    # Archive the files
    for blob in blobs:
        if not blob.name.endswith('/'):
            destination_blob_name = blob.name.replace(archive_config["source_folder_name"], archive_config["destination_folder_name"], 1)

            source_blob = source_bucket.blob(blob.name)
            destination_blob = destination_bucket.blob(destination_blob_name)
            destination_blob.upload_from_string(source_blob.download_as_string())

            source_blob.delete()

spark.stop()