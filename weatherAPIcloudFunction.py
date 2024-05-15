import functions_framework
import requests
import json
from google.cloud import storage
import datetime

@functions_framework.http
def fetch_weather_data(request):
    """Cloud Function entry point to fetch weather data from API for multiple cities and upload it to Google Cloud Storage."""
    cities = ["Hong Kong", "Sydney", "Melbourne", "Glasgow", "Madras"]
    api_key = "b1cf93f3fa8ee1481414f2b6bc767cf4"
    weather_data_list = []
    
    # Loop through each city to fetch the data
    for city in cities:
        url = f"http://api.weatherstack.com/current?access_key={api_key}&query={city}"
        response = requests.get(url)
        if response.status_code == 200:
            weather_data = response.json()
            if weather_data:
                #weather_data['city'] = city  # Adding city name to the data
                weather_data_list.append(weather_data)
            else:
                print(f"No weather data found for {city}")
        else:
            print(f"Failed to fetch weather data for {city}: {response.status_code}")
            continue  # Proceed to the next city if fetching failed
    
    # Upload data to GCS after collecting for all cities
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
    """Upload the weather data to a Google Cloud Storage bucket."""
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"weatherapi/{current_day}"
    file_name = f"weatherapi_data_{current_time}.json"
    bucket = storage_client.bucket('bigdatadump-apitobq')  # Correct bucket name
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')

