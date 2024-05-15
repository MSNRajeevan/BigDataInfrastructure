import functions_framework
import requests
import json
from google.cloud import storage
import datetime

@functions_framework.http
def fetch_weather_data(request):
    """
    A Google Cloud Function that fetches current weather data for a specified list of cities using the weatherstack API
    and uploads the aggregated results to a Google Cloud Storage bucket. This function is triggered by an HTTP request.
    
    Parameters:
    request (flask.Request): The request object provided by the Cloud Function's HTTP trigger.
    
    Returns:
    str: A message indicating whether the upload to Google Cloud Storage was successful or if it failed.
    """
    cities = ["Hong Kong", "Sydney", "Melbourne", "Glasgow", "Madras"]
    api_key = "b1cf93f3fa8ee1481414f2b6bc767cf4"
    weather_data_list = []
    
    for city in cities:
        url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"
        response = requests.get(url)
        if response.status_code == 200:
            weather_data = response.json()
            if weather_data:
                weather_data_list.append(weather_data)
            else:
                print(f"No weather data found for {city}")
        else:
            print(f"Failed to fetch weather data for {city}: {response.status_code}")
            continue
    
    if weather_data_list:
        try:
            storage_client = storage.Client()
            aggregated_data = {"data": weather_data_list}
            upload_to_gcs(aggregated_data, storage_client)
            return "Successfully uploaded to GCS"
        except Exception as e:
            print(f"Failed to upload data to GCS: {e}")
            return "Failed to upload data to GCS"
    else:
        return "No weather data fetched from API for any city"

def upload_to_gcs(data, storage_client):
    """
    Uploads serialized JSON data to a specified path within a Google Cloud Storage bucket. This function formats the 
    data path using the current date and time to avoid overwriting previous uploads and to make data tracking easier.
    
    Parameters:
    data (dict): The data to upload, formatted as a dictionary containing weather information for multiple cities.
    storage_client (google.cloud.storage.Client): The GCS client used to interact with the Google Cloud Storage service.
    
    Returns:
    None: This function does not return any value but will raise exceptions if the upload process fails.
    """
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"weatherapi/{current_day}"
    file_name = f"weatherapi_data_{current_time}.json"
    bucket = storage_client.bucket('bigdatadump-apitobq')
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')