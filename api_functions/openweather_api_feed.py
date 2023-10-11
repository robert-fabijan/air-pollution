from data_extraction import OpenWeatherDataExtractor
# from etl import data_configuration
# from cloud_functions import cloud_integration
import sys
import yaml


class OpenWeatherDataIngestor:

    def __init__(self) -> None:
        self.cities_yaml = 'conf/cities.yaml'
        self.city_datatable_schema = 'conf/city_table_schema.yaml'
        self.unified_city_datatable_schema = 'conf/unified_city_table_schema.yaml'


    def load_cities_from_yaml(self):
        ''' 
        
        Extract data about cities for OpenWeather API calls 
        
        '''
        try:
            with open(self.cities_yaml, 'r') as file:
                data = yaml.safe_load(file)
            return data.get("cities", [])
        except FileNotFoundError:
            print(f"Error: File {self.cities_yaml} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.cities_yaml}.")
        except yaml.YAMLError as exc:
            print(f"Error parsing the YAML file: {exc}.")
        return []
    


    def get_city_coordinates(
            self,
            city_name: str,
            country_code: str,
    ) -> dict:
        """
        
        Coords extraction for specific city name 
        
        """
        data = OpenWeatherDataExtractor().get_geo_direct_cities_data(city_name, country_code)
        if data:
            coords_data = {
                'city_name': data[0]['name'],
                'country_code': data[0]['country'],
                'lat': data[0]['lat'],
                'lon': data[0]['lon'],
            }

            return coords_data





    def get_city_air_pollution_data(
            self,
            lat: float,
            lon: float,
        ) -> dict:
        """
        .
        ..

        """
        data = OpenWeatherDataExtractor().get_air_pollution_data(lat, lon)
        if data:
            air_pollution_data = {
                'datetime': data['list'][0]['dt'],
                'air_components': data['list'][0]['components']
            }
        return air_pollution_data





    def get_city_air_pollution_history_data(
            self,
            lat: float,
            lon: float,
            unix_start_date: int,
            unix_end_date: int
        ) -> dict:
        """

        Function that pulls historical openweather data in json 

        """
        data = OpenWeatherDataExtractor().get_air_pollution_history_data(lat, lon, unix_start_date, unix_end_date)
        if data:
            air_pollution_history_data = {}
            for i in range(0, len(data['list'])):
                air_pollution_history_data[i] = {
                    'datetime': data['list'][i]['dt'],
                    'aqi': data['list'][0]['main']['aqi'],
                    'air_components': data['list'][i]['components']
                }

        return air_pollution_history_data
    

def gcloud_get_openweather_data_function(request, context=None) -> dict:
    '''

    Run a loop through all required cities to extract data 
    and return dictionary with all city data
    
    '''
    OpenWeatherDataIngestorObject = OpenWeatherDataIngestor()
    # data placeholder
    all_city_data = {}

    for city in OpenWeatherDataIngestorObject.load_cities_from_yaml():
        # get lon and lat for city
        coord_data = OpenWeatherDataIngestorObject.get_city_coordinates(city['name'], city['country_code'])
        # get air polluution_data for city
        air_pollution_data = OpenWeatherDataIngestorObject.get_city_air_pollution_data(coord_data['lat'], coord_data['lon'])
        historical_air_pollution = OpenWeatherDataIngestorObject.get_city_air_pollution_history_data(coord_data['lat'], coord_data['lon'], 1696320000, 1696356000)  # Timestamp podane na 3-10-2023 8-18, na próbę


        # append data placeholder
        all_city_data[city['name']] = coord_data
        all_city_data[city['name']]['air_pollution'] = air_pollution_data
        all_city_data[city['name']]['history_air_pollution'] = historical_air_pollution

    return all_city_data

