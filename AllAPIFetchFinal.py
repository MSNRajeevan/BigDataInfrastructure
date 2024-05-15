import pandas as pd
from google.cloud import storage
import requests
import json
import datetime
import functions_framework
from collections import Counter

def download_excel(storage_client):
    """Download Excel file containing airport codes from GCS."""
    bucket = storage_client.bucket('flights_api_data_dump')
    blob = bucket.blob('city_codes/airports-code@public.xlsx')
    blob.download_to_filename('/tmp/airports-code@public.xlsx')
    return '/tmp/airports-code@public.xlsx'

def read_airport_data(file_path):
    """Read the Excel file to get airport data."""
    df = pd.read_excel(file_path)
    return df.set_index('Airport Code')['City Name'].to_dict()

def get_top_cities(processed_data):
    """Process to find top cities from flights api response based on both departure and arrival cities."""
    departure_cities = [flight['departure_city'] for flight in processed_data]
    arrival_cities = [flight['arrival_city'] for flight in processed_data]

    departure_city_counts = Counter(departure_cities)
    arrival_city_counts = Counter(arrival_cities)

    top_departure_cities = [city for city, count in departure_city_counts.most_common(3)]
    top_arrival_cities = [city for city, count in arrival_city_counts.most_common(3)]

    top_cities = list(set(top_departure_cities + top_arrival_cities))

    if len(top_cities) < 6:
        additional_cities = [city for city, count in (departure_city_counts + arrival_city_counts).most_common(6)]
        top_cities = list(set(top_cities + additional_cities))[:6]

    return top_cities


@functions_framework.http
def fetch_all_api(request):
    flights_api_key = "37515365e8ccb5c5e70150cea3f867e4"
    url = f"http://api.aviationstack.com/v1/flights?access_key={flights_api_key}"

    storage_client = storage.Client()
    file_path = download_excel(storage_client)
    airport_dict = read_airport_data(file_path)

    try:
        flights_response = requests.get(url)
        flights_data = flights_response.json()
        if flights_data:
            try:
                processed_data = process_flights_data(flights_data['data'], airport_dict)
                top_cities = get_top_cities(processed_data)
                upload_to_flights_gcs(processed_data, storage_client)
                hotelsAPIFetch(top_cities, request)
                fetch_weather_data(top_cities, request)
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

def hotelsAPIFetch(cities, request):

    api_hotel_data = []
    
    for city in cities:
        geo_id, query = fetch_geo_id(city)
        if geo_id:
              url = "https://tripadvisor16.p.rapidapi.com/api/v1/hotels/searchHotels"
              querystring = {
                "geoId": str(geo_id),
                "checkIn": "2024-05-16",
                "checkOut": "2024-05-21",
                "pageNumber": "1",
                "currencyCode": "USD"
              }
              headers = {
	            "X-RapidAPI-Key": "7014d9e592msh9959e565fdcf09bp11f76djsn3d20223cb1af",
	            "X-RapidAPI-Host": "tripadvisor16.p.rapidapi.com"
                }
              hotels_response = requests.get(url, headers=headers, params=querystring)
              if hotels_response.status_code == 200:
                hotels = hotels_response.json()
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
            upload_to_hotel_gcs(aggregated_data, storage_client)
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
	"X-RapidAPI-Key": "7014d9e592msh9959e565fdcf09bp11f76djsn3d20223cb1af",
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

def fetch_weather_data(cities, request):
    """Cloud Function entry point to fetch weather data from API for multiple cities and upload it to Google Cloud Storage."""
    api_key = "b1cf93f3fa8ee1481414f2b6bc767cf4"
    weather_data_list = []
    
    # Loop through each city to fetch the data
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
    
    # Upload data to GCS after collecting for all cities
    if weather_data_list:
        try:
            storage_client = storage.Client()
            aggregated_data = {"data": weather_data_list}
            upload_to_weather_gcs(aggregated_data, storage_client)
            return "Successfully uploaded to GCS"
        except Exception as e:
            print(f"Failed to upload data to GCS: {e}")
            return "Failed to upload data to GCS"
    else:
        return "No weather data fetched from API for any city"

def upload_to_weather_gcs(data, storage_client):
    """Upload the weather data to a Google Cloud Storage bucket."""
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"weatherapi/{current_day}"
    file_name = f"weatherapi_data_{current_time}.json"
    bucket = storage_client.bucket('bigdatadump-apitobq')  # Correct bucket name
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')

def process_flights_data(data, airport_dict):
    """Flatten the nested JSON data to a simpler format, including city names."""
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

def upload_to_flights_gcs(data, storage_client):
    """Upload the data to a GCS bucket."""
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"flightsapi/{current_day}"
    file_name = f"flightsapi_data_{current_time}.json"
    bucket = storage_client.bucket('flights_api_data_dump')
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')

def upload_to_hotel_gcs(data, storage_client):
    """Upload the hotel data to a Google Cloud Storage bucket."""
    current_day = datetime.date.today().isoformat()
    current_time = datetime.datetime.now().isoformat(timespec='seconds').replace(':', '-')
    file_path = f"hotelapi/{current_day}"
    file_name = f"hotelapi_data_{current_time}.json"
    bucket = storage_client.bucket('hotel_api_data_dump')
    blob = bucket.blob(f"{file_path}/{file_name}")
    blob.upload_from_string(json.dumps(data), content_type='application/json')