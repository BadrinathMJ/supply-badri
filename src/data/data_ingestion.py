import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split 
import yaml
import logging
from pathlib import Path
#Logging Configuration
logger = logging.getLogger('data_ingestion')
logger.setLevel('DEBUG')

console_handler = logging.StreamHandler()
console_handler.setLevel('DEBUG')
file_handler = logging.FileHandler('errors.log')
file_handler.setLevel('ERROR')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)
FILES = {'sales':'sales_history.csv','promos':'promotions.csv','inventory':'inventory_snapshot.csv','transportation_cost':'transportation_cost.csv','sku':'sku_master.csv','stores':'stores.csv','dcs':'dcs.csv','holding_cost':'holding_cost_store.csv','penalty_cost':'penalty_cost_store.csv','dc_capacity':'dc_capacity.csv'}

#Helper Functions

def load_params(params_path:str)->dict:
    """Load parameters from YAML file"""
    try:
        with open(params_path, 'r') as file:
            params = yaml.safe_load(file)

        logger.debug('Parameters loaded from %s', params_path)
        return params
    except Exception as e:
        logger.error('Failed to load params: %s', e)
        raise

def load_raw(RAW) -> dict:
    """Load raw time series data"""

    try:
        d={}
        for k,f in FILES.items():
            parse=['week_start'] if k in ['sales','promos','dc_capacity'] else None
            d[k]=pd.read_csv(f'{RAW}/{f}', parse_dates=parse) if parse else pd.read_csv(f'{RAW}/{f}')
        return d
        
    except Exception as e:
        logger.error('Error in loading data %s', e)

def save_interim(data,INTERIM):
    for k,df in data.items(): 
        df.to_csv(f'{INTERIM}/{k}.csv', index=False)

if __name__=='__main__':
    
    params = load_params('../.././params.yaml')
    BASE_DIR = Path(__file__).resolve().parent.parent  # project root
    RAW = params['data_ingestion']['raw_data_folder']
    RAW_DATA_PATH = BASE_DIR / RAW
    INTERIM = params['data_ingestion']['interim_data_folder']
    INTERIM_DATA_PATH = BASE_DIR / INTERIM
    d=load_raw(RAW_DATA_PATH)
    save_interim(d,INTERIM_DATA_PATH)
    print('Extracted → data/interim/')