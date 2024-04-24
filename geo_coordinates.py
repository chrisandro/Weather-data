import requests
import json
import yaml

with open('params.yaml', 'r') as file:
    params = yaml.safe_load(file)

def get_geo_loc(city, api_key=params['api_key']):
    """
    This function sends name-specific requests to the API for obtaining the latitude and longitude values.
    """

    url = f'http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={api_key}'
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers)
    response = json.loads(response.text)

    return response[0]['lat'], response[0]['lon']
