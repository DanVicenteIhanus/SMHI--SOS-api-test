import pandas as pd, numpy as np, scipy.stats as st, tensorflow as tf, keras, json, requests, sys
from matplotlib import pyplot as plt
from geopy import distance
from datetime import datetime as dt
from datetime import timedelta as td
import time
from io import StringIO

'''
===== Main Script =====
    - Testing SMHI and SOS api's
    - Input specified mushroom and get 1000 observations of that mushroom
    - Dates of observation and their locations are obtained
    - The script then finds the closest SMHI weather station
'''

'''
    NOTE: In the SMHI api, these are some of the variables (parameters):
            - '26': Lufttemperatur min (2 x day = 6am, 18pm)
            - '27': Lufttemperatur max (2 x day = 6am, 18pm)
            - '1': Lufttemperatur momentan värde (1 x hour) <- do we want to separate night-temp and day-temp?
            - '17': Nederbörd, (2 x day = 6am, 18pm)
            - '38': Nederbördsintensitet, max of mean (4 x hour)
            - '5': Nederbördsmängd, sum per day (1 x day = 18pm)
            - '6': Relativ luftfuktighet (1 x hour)
            - '39': Daggpunktstemperatur (1 x hour)
'''

def SOS_api_call(species: str, kingdom: str):
    ''' -- Get species observation --
    Input: species [str], kingdom [str]
    Output: 1000 observations (dates and locations) [Pandas Dataframe]
    '''
    try:
        params = {
            'kingdom': kingdom,
            'scientificName': species, 
            'translationCultureCode': 'en-GB',
            'sensitiveObservations': 'false',
            'skip': '0',
            'take': '1000',
            'sortOrder': 'Asc'
        }
        
        url = 'https://api.artdatabanken.se/species-observation-system/v1/Observations/Search/DwC'

        headers = {
            'X-Api-Version': '1.5',
            'Cache-Control': 'no-cache',
            'Ocp-Apim-Subscription-Key': 'INSERT SUBSCRIPTION KEY HERE',
            'Authorization': 'Bearer INSERT AUTHORIZATION HERE',
            }
        
        r = requests.get(url, headers=headers, params=params)
        data = pd.DataFrame.from_dict(r.json())
        data = data[['eventDate', 'county', 'municipality', 'locality',
                      'decimalLatitude', 'decimalLongitude', 'scientificName', 'vernacularName']]
        return data

    except Exception as e:
        print(e)
        sys.exit()

def SMHI_get_weather_locations():
    ''' -- Get all available weather stations in Sweden for a given set of parameters
    Output: Positions and ids of weather stations [Pandas dataframe]
    '''
    # -- Endpoint -- #
    met_obs_api = 'https://opendata-download-metobs.smhi.se/api/version/latest/'
    
    # -- Variables -- #
    temperature             = 'parameter/1.json?measuringStations=core'
    rain_categorical        = 'parameter/18.json?measuringStations=core'
    rain_intensity          = 'parameter/38.json?measuringStations=core'
    rain_amount             = 'parameter/7.json?measuringStations=core'
    humidity                = 'parameter/6.json?measuringStations=core'

    try:
        # -- Requests -- #
        r_temp             = requests.get(url = met_obs_api + temperature)
        r_rain_categorical = requests.get(url = met_obs_api + rain_categorical)
        r_rain_intensity   = requests.get(url = met_obs_api + rain_intensity)
        r_rain_amount      = requests.get(url = met_obs_api + rain_amount)
        r_humidity         = requests.get(url = met_obs_api + humidity)
        reqs = [r_temp, r_rain_categorical, r_rain_intensity, r_rain_amount, r_humidity]
        #data_arr = []
        
        # -- Aggregate requests to dataframe -- #
        for i, req in enumerate(reqs):
            stations = req.json().get('station',[])
            data = pd.json_normalize(stations)
            data = data[['id', 'key', 'active', 'latitude', 'longitude']]
            #data_arr.append(data)
            if i == 0:
                agg_data = data
            else:
                agg_data = agg_data[agg_data['key'].isin(data['key'])]
        agg_data.index = np.arange(agg_data.shape[0])    
        return agg_data
    
    except Exception as e:
        print(e)
        sys.exit()

def get_closest_met_station(station_data: pd.DataFrame, obs_coordinate): 
    ''' -- Computes the closest weather station using geodistance -- 
    Input: Available weather stations [Pandas dataframe], observation coordinate [(float, float) or (string,string)] 
    Output: Id of closest station [str]
    '''
    n_stations = station_data.shape[0]
    distances = np.zeros((n_stations, 2)) # each row is a distance and the key/id for that station

    for i, station in station_data.iterrows():
        station_coordinate = (float(station['latitude']), float(station['longitude']))
        distances[i, 0] = distance.distance(station_coordinate, obs_coordinate).kilometers
        distances[i, 1] = station['id']

    min_distance_index = np.argmin(distances[:, 0])
    closest_station = station_data.iloc[min_distance_index]['id']
    return closest_station


def get_stations_and_dates(station_data: pd.DataFrame, obs_data: pd.DataFrame):
    ''' -- Find the closest station to each observation -- 
    Input: available stations [Pandas Dataframe], observations [Pandas Dataframe]
    Output: station id's and associated observation date [ dict =  {'station_id': obs_date} ]
    '''
    stations = {}
    for i, obs in obs_data.iterrows():
        pos_latitude = fungi_data.iloc[i]['decimalLatitude']
        pos_longitude = fungi_data.iloc[i]['decimalLongitude']
        obs_coordinate = (float(pos_latitude), float(pos_longitude))
        closest_station = get_closest_met_station(station_data=station_data, 
                                                obs_coordinate=obs_coordinate)
        obs_date = obs['eventDate']
        if closest_station in stations:
            stations[closest_station].append(obs_date)
        else:
            stations[closest_station] = [obs_date]

    return stations

def get_dates(station_obs: dict, days_back: int):
    ''' -- Convert dates associated with observation and parse between which dates we want weather --
    Input: station ids & date [dict], how many days back we want data [int]
    Output: The station id & date interval we need weather obs from [Pandas Dataframe]
    '''
    rows_list = []
    for id in station_obs:
        for date in station_obs[id]:
            sp = date.split('/')[0]
            sp = sp.split('T')[0]
            end_date = dt.strptime(sp, '%Y-%m-%d')
            start_date = (end_date - td(days = days_back))
            entry = {'id': id, 
                     'start_date': start_date.strftime('%Y-%m_%d'),
                       'end_date': end_date.strftime('%Y-%m_%d')}
            rows_list.append(entry)
    dates = pd.DataFrame(rows_list)
    dates.reset_index(drop=True, inplace=True)
    return dates

if __name__ == '__main__':
    species = 'Cantharellus cibarius'
    kingdom = 'fungi'
    days_back = 14 # how many days before obs to get weather
    fungi_data = SOS_api_call(species, kingdom)
    weather_stations = SMHI_get_weather_locations()
    station_obs = get_stations_and_dates(station_data=weather_stations, 
                                      obs_data=fungi_data)
    dates = get_dates(station_obs=station_obs, 
                      days_back=days_back)
    
