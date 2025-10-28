import pandas as pd, os
import yaml
import logging
from pathlib import Path
#Logging Configuration
logger = logging.getLogger('data_preprocessing')
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

def preprocess(PREPROCESSED,INTERIM):
    sales = pd.read_csv(f'{INTERIM}/sales.csv', parse_dates=['week_start'])
    promos = pd.read_csv(f'{INTERIM}/promos.csv', parse_dates=['week_start'])
    sku = pd.read_csv(f'{INTERIM}/sku.csv')
    stores = pd.read_csv(f'{INTERIM}/stores.csv')
    sales = sales[(sales.qty_sold >= 0) & (sales.qty_sold < 100000)]
    sales = sales[sales['sku_id'].isin(sku['sku_id'])]
    sales = sales[sales['store_id'].isin(stores['store_id'])]
    sales['qty_sold'] = sales['qty_sold'].fillna(0)
    promos['is_promo']=promos['is_promo'].fillna(0).astype(int)
    promos['discount_pct'] = promos['discount_pct'].fillna(0)
    sales.to_csv(f'{PREPROCESSED}/sales_clean.csv', index=False)
    promos.to_csv(f'{PREPROCESSED}/promos_clean.csv', index=False)

    for name in ['inventory','transportation_cost','sku','stores','dcs','holding_cost','penalty_cost','dc_capacity']:
        interim = pd.read_csv(f'{INTERIM}/{name}.csv')
        interim.to_csv(f'{PREPROCESSED}/{name}_clean.csv', index=False)

if __name__ == "__main__":
    params = load_params('../../params.yaml')
    BASE_DIR = Path(__file__).resolve().parent.parent  # project root
    INTERIM = params['data_ingestion']['interim_data_folder']
    INTERIM_DATA_PATH = BASE_DIR / INTERIM
    PREPROCESSED = params['data_preprocessing']['preprocessed_folder']
    PREPROCESSED_DATA_PATH = BASE_DIR / PREPROCESSED
    preprocess(PREPROCESSED_DATA_PATH,INTERIM_DATA_PATH)
    print('Cleaned->data/preprocessed')