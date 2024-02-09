import functions_framework
import sys
import os
import json
import urllib.parse
import base64

sys.path.append(os.path.dirname(os.path.abspath(__file__)))  # add gcloud_functions
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # add gcloud


from shared.models.openweather_transformator import OpenWeatherDataIngestor
from shared.utils import DataConfigurator
from shared.etl.data_transformation import OpenWeatherHistoricalDataTransformator



    
@functions_framework.http
def gcloud_get_openweather_data_function(request, context=None) -> dict:
    """

    Run a loop through all required cities to extract data 
    and return dictionary with all city data

    :param request:
    :param context:
    :return dict: historic air-pollution data for all cities 
    
    """
    # Data Extrat->Transform Object
    OpenWeatherDataIngestorObject = OpenWeatherDataIngestor()


    # Utils object
    DataConfiguratorObject = DataConfigurator()
    START, END = DataConfiguratorObject.timeframe_window()
    
    all_city_data = {}

    for city in DataConfiguratorObject.load_cities_from_yaml():
        # get lon and lat for city
        coord_data = OpenWeatherDataIngestorObject.get_city_coordinates(city['name'], city['country_code'])
        # get air polluution_data for city
        air_pollution_data = OpenWeatherDataIngestorObject.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
        historical_air_pollution = OpenWeatherDataIngestorObject.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], START, END)  


        # append data placeholder
        all_city_data[city['name']] = coord_data
        all_city_data[city['name']]['air_pollution'] = air_pollution_data
        all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution

    return str(all_city_data)


@functions_framework.http
def gcloud_transform_api_message(request, context=None) -> None:
    """
    
    Run a etl script to transform a dict string data from pubsub message. 
    It calls OpenWeatherHistoricalDataTransformator class and uses pandas to perform
    transformation.

    :param request:
    :param context:
    :return pandas.DataFrame: clean dataframe with air pollution data 
    """
    # print(type(request))
    # print(request.get_json())
    # Your JSON data as a string

    # Extract the encoded inner JSON string
    encoded_inner_json = request.get_json()['data']['data']
    print("PRINT1", encoded_inner_json)
    # Decode the inner JSON string
    decoded_inner_json = base64.b64decode(encoded_inner_json).decode('utf-8')
    print("PRINT2",decoded_inner_json)
    # Parse the decoded JSON string
    inner_data = json.loads(decoded_inner_json)
    print("PRINT3",inner_data)
    return OpenWeatherHistoricalDataTransformator().historic_data_transform(inner_data)










