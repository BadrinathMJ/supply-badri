import pandas as pd, os
import yaml
import logging
from pathlib import Path
#Logging Configuration
logger = logging.getLogger('feature_engineering')
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

def add_time(df):
    df['weekofyear']=df['week_start'].dt.isocalendar().week.astype(int)
    df['month']=df['week_start'].dt.month.astype(int)
    df['year']=df['week_start'].dt.year.astype(int)
    return df
def add_lags(df, lags=[1,2,4,8]):
    df=df.sort_values(['store_id','sku_id','week_start'])
    for l in lags: 
        df[f'lag_{l}']=df.groupby(['store_id','sku_id'])['qty_sold'].shift(l)
    return df
def add_ma(df, windows=[4,8]):
    for w in windows: 
        df[f'ma_{w}']=df.groupby(['store_id','sku_id'])['qty_sold'].transform(lambda s: s.rolling(w, min_periods=2).mean())
    return df
def build(PREPROCESSED, PROCESSED):
    sales=pd.read_csv(f'{PREPROCESSED}/sales_clean.csv', parse_dates=['week_start'])
    promos=pd.read_csv(f'{PREPROCESSED}/promos_clean.csv', parse_dates=['week_start'])
    df=sales.merge(promos, on=['store_id','sku_id','week_start'], how='left')
    df['is_promo']=df['is_promo'].fillna(0).astype(int)
    df['discount_pct']=df['discount_pct'].fillna(0)
    df=add_time(df)
    df=add_lags(df)
    df=add_ma(df)
    df.to_csv(f'{PROCESSED}/features_store_sku_week.csv', index=False)
    return df

if __name__ == "__main__":
    params = load_params('../../params.yaml')
    BASE_DIR = Path(__file__).resolve().parent.parent  # project root
    PREPROCESSED = params['data_preprocessing']['preprocessed_folder']
    
    PREPROCESSED_DATA_PATH = BASE_DIR / PREPROCESSED
    PROCESSED = params['feature_engineering']['processed_folder']
    PROCESSED_DATA_PATH = BASE_DIR / PROCESSED
    df = build(PREPROCESSED_DATA_PATH,PROCESSED_DATA_PATH)
    print(f'✅ Features → {PREPROCESSED_DATA_PATH}/features_store_sku_week.csv shape={df.shape}')