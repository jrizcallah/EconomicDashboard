import pandas as pd
import requests
import re
import json
import logging
from datetime import datetime
from config import *

logger = logging.getLogger(__name__)
logging.basicConfig(filename='load_data.log', 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def download_and_save_business_entity_data(url_file, file_path):
    if not ((file_path[-8:] == '.parquet') or (file_path[-4:] == '.csv')):
        raise ValueError('file_path must be .parquet or .csv') 
    
    with open(url_file, 'r') as file:
        data_url = file.read()
    
    logger.info(f'Getting data from {data_url}')
    r = requests.get(data_url)
    logger.info(f'Request returned {r}')
    r_json = json.loads(r.content)
    data = pd.DataFrame.from_records(r_json)
    logger.info(f'DataFrame has {data.shape[0]} rows')

    save_data(data, file_path)
    return data


def download_and_save_business_statistics_data(url_file, file_path):
    if not ((file_path[-8:] == '.parquet') or (file_path[-4:] == '.csv')):
        raise ValueError('file_path must be .parquet or .csv') 
    
    with open(url_file, 'r') as file:
        data_url = file.read()

    logger.info(f'Getting data from {data_url}')
    r = requests.get(data_url)
    logger.info(f'Request returned {r}')
    l = re.split('(Period,Value)', str(r.content))
    l = ''.join(l[1:])
    l = l.replace('\\r', '')
    l = l.replace('\\n', ' ')
    l = l.split()

    data = pd.DataFrame.from_records([i.split(',') for i in l[1:]])
    data.columns = l[0].split(',')
    data.dropna(inplace=True)
    data = data[data['Value'] != 'NA']
    logger.info(f'DataFrame has {data.shape[0]} rows')

    save_data(data, file_path)
    return data


def load_data(file_path):
    logging.info(f'Loading data from {file_path}')
    if file_path[-8:] == '.parquet':
        data = pd.read_parquet(file_path)
    elif file_path[-4:] == '.csv':
        data = pd.read_parquet(file_path)
    else:
        raise ValueError('file must be .parquet or .csv')
    return data


def save_data(data, file_path):
    logging.info(f'Loading data from {file_path}')
    if file_path[-8:] == '.parquet':
        data.to_parquet(file_path)
    elif file_path[-4:] == '.csv':
        data.to_csv(file_path)
    else:
        raise ValueError('file must be .parquet or .csv')
    return data


def clean_business_entity_data(data):
    logging.info('Cleaning business entity data')
    data['entityformdate'] = pd.to_datetime(data['entityformdate'])
    data['count_entityid'] = data['count_entityid'].astype(int)
    data['principalcity'] = data['principalcity'].str.capitalize()
    data.loc[data['principalcity'] == 'None', 'principalcity'] = 'Unknown'
    logging.info(f'Column [principalcity] has {data["principalcity"].isna().sum()} missing values')
    data['principalcity'] = data['principalcity'].fillna('Unknown')
    
    logging.info(f'Column [principalzipcode] has {data["principalzipcode"].isna().sum()} missing values')
    data.loc[data['principalzipcode'] == 'None', 'principalzipcode'] = 'Unknown'
    data['principalzipcode'] = data['principalzipcode'].fillna('Unknown')

    data['goodstanding'] = data['entitystatus'] == 'Good Standing'
    return data


def clean_business_statistics_data(data):
    logger.info('Cleaning business statistics data')
    data['Period'] = pd.to_datetime(data['Period'], format='%b-%Y')
    data['Value'] = data['Value'].astype(int)
    logger.info(f'Column [Period] has {data["Period"].isna().sum()} missing values')
    logger.info(f'Column [Value] has {data["Value"].isna().sum()} missing values')
    data.dropna(inplace=True)
    return data
    

if __name__ == '__main__':
    logger.info('Function: load_and_save_business_entity_data')
    business_entity_data = download_and_save_business_entity_data(BUSINESS_ENTITY_URL_FILE, 
                                                                  BUSINESS_ENTITY_PARQUET_PATH)
    logger.info('Function: load_and_save_business_statistics_data')
    business_statistics_data = download_and_save_business_statistics_data(BUSINESS_STATISTICS_URL_FILE, 
                                                                          BUSINESS_STATISTICS_PARQUET_PATH)
    logger.info('Functionn: clean_business_entity_data')
    business_entity_data = clean_business_entity_data(business_entity_data)
    logger.info('Function: clean_business_statistics_data')
    business_statistics_data = clean_business_statistics_data(business_statistics_data)

    save_data(business_entity_data, BUSINESS_ENTITY_PARQUET_PATH)
    save_data(business_statistics_data, BUSINESS_STATISTICS_PARQUET_PATH)
