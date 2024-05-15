import pandas as pd
from google.cloud import storage
import requests
import json
import datetime
import functions_framework

def download_excel(storage_client):
    """
    Download an Excel file containing airport codes from a Google Cloud Storage bucket.

    This function is specifically intended to be run in the Google Cloud environment,
    where it accesses a specified bucket to retrieve the file.

    Args:
        storage_client (google.cloud.storage.Client): A client to interact with Google Cloud Storage.

    Returns:
        str: The local path to the downloaded Excel file.
    """
    bucket = storage_client.bucket('flights_api_data_dump')
    blob = bucket.blob('city_codes/airports-code@public.xlsx')
    blob.download_to_filename('/tmp/airports-code@public.xlsx')
    return '/tmp/airports-code@public.xlsx'

def read_airport_data(file_path):
    """
    Read an Excel file to extract and map airport codes to city names.

    Designed to operate within the Google Cloud environment, ensuring compatibility with
    cloud storage for file operations.

    Args:
        file_path (str): The path to the Excel file.

    Returns:
        dict: A dictionary mapping airport codes to city names.
    """
    df = pd.read_excel(file_path)
    return df.set_index('Airport Code')['City Name'].to_dict()

@functions_framework.http
def fetch_flights(request):
    """
    Cloud function to fetch flight data from an API, process it, and upload to Google Cloud Storage.

    This function orchestrates various operations including API data retrieval, data processing,
    and uploading to Google Cloud Storage, tailored for execution on the Google Cloud Platform.

    Args:
        request (flask.Request): The request object providing request details.

    Returns:
        str: A message indicating the success or failure of the operation.
    """

    api_key = "37515365e8ccb5c5e70150cea3f867e4"
    url = f"http://api.aviationstack.com/v1/flights?access_key={api_key}"

    storage_client = storage.Client()
    file_path = download_excel(storage_client)
    airport_dict = read_airport_data(file_path)

    try:
        response = requests.get(url)
        data = response.json()
        if data:
            try:
                processed_data = process_data(data['data'], airport_dict)
                upload_to_gcs(processed_data, storage_client)
                return "Successfully uploaded to GCS"
            except Exception as e:
                print(f"Failed to upload data to GCS: {e}")
                return "Failed to upload data to GCS"
        else:
            print("No Data!")
            return "No Data fetched from API"
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return "Error fetching data from API"

def process_data(data, airport_dict):
    """
    Process raw JSON data from flight API into a structured format by mapping airport codes to city names.

    This function is optimized for Google Cloud functions, enabling efficient data transformation
    before storage or further processing.

    Args:
        data (list of dict): Raw data from the flight API.
        airport_dict (dict): Dictionary mapping airport codes to city names.

    Returns:
        list of dict: Processed data with detailed flight information and mapped city names.
    """
    flattened_data = []
    for entry in data:
        departure_iata = entry['departure']['iata']
        arrival_iata = entry['arrival']['iata']
        flattened_data.append({
            "flight_date": entry.get("flight_date", ""),
            "flight_status": entry.get("flight_status", ""),
            "departure_airport": entry['departure']['airport'],
            "departure_city": airport_dict.get(departure_iata, "Unknown"),
            "departure_scheduled": entry['departure']['scheduled'],
            "departure_estimated": entry['departure']['estimated'],
            "departure_delay": entry['departure'].get('delay', ""),
            "arrival_airport": entry['arrival']['airport'],
            "arrival_city": airport_dict.get(arrival_iata, "Unknown"),
            "arrival_scheduled": entry['arrival']['scheduled'],
            "arrival_estimated": entry['arrival']['estimated'],
            "arrival_delay": entry['arrival'].get('delay', ""),
            "flight_name": entry['flight']['number'],
            "airline_name": entry['airline']['name']
        })
    return flattened_data

def upload_to_gcs(data, storage_client):
    """
    Upload structured flight data to a Google Cloud Storage bucket.

    Designed for seamless execution within Google Cloud functions, this method manages the upload of data
    ensuring it is stored securely and efficiently on Google Cloud Storage.

    Args:
        data (dict): Processed flight data to upload.
        storage_client (google.cloud.storage.Client): A client to interact with Google Cloud Storage.
    """

    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"flightsapi/{current_day}"
    file_name = f"flightsapi_data_{current_time}.json"
    bucket = storage_client.bucket('flights_api_data_dump')
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')