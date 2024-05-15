from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import ArrayType
from google.cloud import storage
import datetime

# Initialize SparkSession with BigQuery support.
spark = SparkSession.builder \
    .appName('FinalHotelsAPIDataToBigQuery') \
    .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0') \
    .getOrCreate()

current_date = datetime.date.today().isoformat()

hotel_json_path = f'gs://hotel_api_data_dump/hotelapi/{current_date}/*'

storage_client = storage.Client()

archive_config = {
    "source_bucket_name": "hotel_api_data_dump",
    "source_folder_name": f"hotelapi/{current_date}",
    "destination_bucket_name": "hotel_api_data_archive",
    "destination_folder_name": "hotel_archives/"
}

source_bucket = storage_client.bucket(archive_config["source_bucket_name"])

# Get all the blobs (files) in the source folder.
blobs = list(source_bucket.list_blobs(prefix=archive_config["source_folder_name"]))

# Check if there are any files in the specified path.
if not blobs:
    print("No data found in the specified folder.")
else:
    # Read the JSON files into a DataFrame.
    hotel_df = spark.read.json(hotel_json_path)

    if isinstance(hotel_df.schema["data"].dataType, ArrayType):
        hotels_before_flattening_df = hotel_df.select(explode(col("data")).alias("hotel"))
    else:
        hotels_before_flattening_df = hotel_df.select(col("data").alias("hotel"))

    flattened_hotel_df = hotels_before_flattening_df.select(
        col("hotel.city").alias("hotel_city"),
        col("hotel.id").alias("hotel_id"),
        col("hotel.title").alias("hotel_title"),
        col("hotel.bubbleRating.count").alias("ratings_count"),
        col("hotel.bubbleRating.rating").alias("rating"),
        col("hotel.provider").alias("provider"),
        col("hotel.priceForDisplay").alias("price"),
        col("hotel.priceDetails").alias("additional_details"),
        col("hotel.commerceInfo.externalUrl").alias("external_url")
    )

    flattened_hotel_df.show(truncate=False)

    flattened_hotel_df.write.format('bigquery') \
        .option('table', 'scenic-style-420014.BigData767.hotelsTable') \
        .option('temporaryGcsBucket', 'hotels_tmp_dataproc_dmp') \
        .mode('overwrite') \
        .save()

    # Archive the files
    destination_bucket = storage_client.bucket(archive_config["destination_bucket_name"])

    for blob in blobs:
        if not blob.name.endswith('/'):
            destination_blob_name = blob.name.replace(archive_config["source_folder_name"], archive_config["destination_folder_name"], 1)

            source_blob = source_bucket.blob(blob.name)
            destination_blob = destination_bucket.blob(destination_blob_name)
            destination_blob.upload_from_string(source_blob.download_as_string())

            source_blob.delete()

spark.stop()