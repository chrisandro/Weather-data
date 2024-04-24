import geo_coordinates
import requests
import schedule
import time
import datetime
import yaml
import json
import os

'''
This script sends scheduled requests to OpenWeatherMap API and saves the JSONs to the path defined in the params.yaml. 
The filenames start with the name of the city followed by the date-timestamp of the download. After download the data 
is ingested to an internal stage in Snowflake. Script will stay in running mode until stopped by the user.
'''

with open('params.yaml', 'r') as file:
    params = yaml.safe_load(file)

API_KEY = params['api_key']
FOLDER_PATH = params['data_folder_path']
CITY_DATA = {'Milan': geo_coordinates.get_geo_loc('Milan'), 'Bologna': geo_coordinates.get_geo_loc('Bologna'), 'Cagliari': geo_coordinates.get_geo_loc('Cagliari')}

def make_api_request():
    """
    A function that sends three requests (one for each city) 
    to the weather API for fetching the current weather data.
    """
    
    for c in CITY_DATA:
        api_endpoint = f'https://api.openweathermap.org/data/2.5/weather?lat={CITY_DATA[c][0]}&lon={CITY_DATA[c][1]}&appid={API_KEY}'
        headers = {"Accept": "application/json"}
        
        try:
            # Make the API request
            response = requests.get(api_endpoint, headers=headers)
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            file_path = os.path.join(FOLDER_PATH, f'{c}_{timestamp}.json')

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                print(f"API request successful at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

                save_file = open(file_path, "w")
                json.dump(response.json(), save_file)  
                save_file.close()  

            else:
                print(f"API request failed with status code: {response.status_code}")
        except Exception as e:
            print(f"Error making API request: {e}")



if __name__ == "__main__":
    
    make_api_request()
    # Schedule the API request to run every one hour
    schedule.every().hour.do(make_api_request)

    # Run the scheduled tasks
    while True:
        schedule.run_pending()
        time.sleep(1)    
    