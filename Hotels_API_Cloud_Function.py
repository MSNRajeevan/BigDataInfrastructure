import functions_framework
import requests
import json
from google.cloud import storage
import datetime

@functions_framework.http
def hotelsAPIFetch(request):
    """Cloud Function entry point to fetch hotel data from API for multiple cities and upload it to Google Cloud Storage."""
    cities = ["Hong Kong","Sydney", "Melbourne", "Glasgow" , "Madras" , "Chicago"]

    api_hotel_data = []
    
    for city in cities:
        geo_id, query = fetch_geo_id(city)
        if geo_id:
              url = "https://tripadvisor16.p.rapidapi.com/api/v1/hotels/searchHotels"
              querystring = {
                "geoId": str(geo_id),
                "checkIn": "2024-05-14",
                "checkOut": "2024-05-19",
                "pageNumber": "1",
                "currencyCode": "USD"
              }
              headers = {
                  "X-RapidAPI-Key": "b68f1a7e62msh378aeed39676b4ep141ca1jsn78d81b9a905c",
                  "X-RapidAPI-Host": "tripadvisor16.p.rapidapi.com"
              }
              response = requests.get(url, headers=headers, params=querystring)
              if response.status_code == 200:
                hotels = response.json()
                if 'data' in hotels and 'data' in hotels['data']:
                  for hotel in hotels['data']['data']:
                    hotel['city'] = city
                    api_hotel_data.append(hotel)
                else:
                    print(f"No hotel data found for {city}")
        else:
            print(f"No GeoID fetched for {city}")
            continue

    if api_hotel_data:
        try:
            storage_client = storage.Client()
            aggregated_data = {"data": api_hotel_data}
            upload_to_gcs(aggregated_data, storage_client)
            return "Successfully uploaded to GCS"
        except Exception as e:
            print(f"Failed to upload data to GCS: {e}")
            return "Failed to upload data to GCS"
    else:
        return "No data fetched from APIs for any city"

def fetch_geo_id(query):
    """Fetch geoId from the TripAdvisor API for a given city name."""
    url = "https://tripadvisor16.p.rapidapi.com/api/v1/hotels/searchLocation"
    querystring = {"query": query}
    headers = {
        "X-RapidAPI-Key": "b68f1a7e62msh378aeed39676b4ep141ca1jsn78d81b9a905c",
        "X-RapidAPI-Host": "tripadvisor16.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        data = response.json()
        if data['data']:
            geo_id = data['data'][0]['geoId']
            return geo_id, query
        else:
            print(f"No geoId found for {query}")
            return None, None
    else:
        print(f"Failed to fetch geoId for {query}: {response.status_code}, {response.text}")
        return None, None


def upload_to_gcs(data, storage_client):
    """Upload the hotel data to a Google Cloud Storage bucket."""
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"hotelapi/{current_day}"
    file_name = f"hotelapi_data_{current_time}.json"
    bucket = storage_client.bucket('hotel_api_data_dump')
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')