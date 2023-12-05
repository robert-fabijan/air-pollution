import yaml
import os
import datetime

class DataConfigurator:


    def __init__(self) -> None:
        self.cities_yaml = os.path.join(os.path.dirname(__file__) , 'configs/cities.yaml') 
        


    def load_cities_from_yaml(self):
        ''' Extract data about cities for OpenWeather API calls '''
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
    

    def timeframe_window(self) -> (int, int):
        end = datetime.datetime.now()
        start = end - datetime.timedelta(days = 1)

        return int(datetime.datetime.timestamp(start)), int(datetime.datetime.timestamp(end))
    

