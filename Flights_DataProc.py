from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage
import datetime


spark = SparkSession.builder \
    .appName('FinalFlightAPIDataToBigQuery') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0') \
    .getOrCreate()

current_date = datetime.date.today().isoformat()


json_path = f'gs://flights_api_data_dump/flightsapi/{current_date}/*'


storage_client = storage.Client()

archive_config = {
    "source_bucket_name": "flights_api_data_dump",
    "source_folder_name": f"flightsapi/{current_date}",
    "destination_bucket_name": "flights_api_data_archive",
    "destination_folder_name": "flights_archive/{current_date}"
}


source_bucket = storage_client.bucket(archive_config["source_bucket_name"])
destination_bucket = storage_client.bucket(archive_config["destination_bucket_name"])


blobs = list(source_bucket.list_blobs(prefix=archive_config["source_folder_name"]))


if not blobs:
    print("No data found in the specified folder.")
else:
    
    full_df = spark.read.json(json_path)

    
    flattened_df = full_df.select(
        col("flight_date").alias("flight_date"),
        col("flight_status").alias("flight_status"),
        col("departure_airport").alias("departure_airport"),
        col("departure_city").alias("departure_city"),
        col("departure_scheduled").alias("departure_scheduled"),
        col("departure_delay").alias("departure_delay_mins"),
        col("arrival_airport").alias("arrival_airport"),
        col("arrival_city").alias("arrival_city"),
        col("arrival_scheduled").alias("arrival_scheduled"),
        col("arrival_delay").alias("arrival_delay_mins"),
        col("flight_name").alias("flight_name"),
        col("airline_name").alias("airline_name")
    )

    
    flattened_df.show(truncate=False)

    
    flattened_df.write.format('bigquery') \
        .option('table', 'scenic-style-420014.BigData767.flightsTable') \
        .option('temporaryGcsBucket', 'flightsapidatatmp') \
        .mode('overwrite') \
        .save()

    
    for blob in blobs:
        if not blob.name.endswith('/'):
            destination_blob_name = blob.name.replace(archive_config["source_folder_name"], archive_config["destination_folder_name"], 1)

            
            source_blob = source_bucket.blob(blob.name)
            destination_blob = destination_bucket.blob(destination_blob_name)
            destination_blob.upload_from_string(source_blob.download_as_string())

            
            source_blob.delete()

spark.stop()