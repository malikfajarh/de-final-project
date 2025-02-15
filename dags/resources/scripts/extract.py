import pandas as pd
import ast
import re
import os
import pytz

def safe_parse(data):
    if pd.isna(data):
        return {}
    try:
        return ast.literal_eval(data)
    except:
        return {}

def to_snake_case(string):
    string = re.sub(r'([a-z])([A-Z])', r'\1_\2', string)  
    return string.lower() 

def extract_table(table, **kwargs):
    data_interval_start = kwargs['data_interval_start']
    data_interval_end   = kwargs['data_interval_end']
    ingestion_mode      = kwargs["params"][table]

    metadata = {'transaction': 'product_metadata', 'click_activity': 'event_metadata'}

    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dir, '..', 'csv', f'{table}.csv')

    pd.set_option('display.max_columns', None)
    df = pd.read_csv(csv_path, error_bad_lines=False)
    # print(df.head(5))

    df.columns = [to_snake_case(col) for col in df.columns]

    if table in metadata:
        df[metadata[table]] = df[metadata[table]].apply(safe_parse)

        if table == 'transaction':
            df = df.explode(metadata[table])    
            df["created_at"] = pd.to_datetime(df["created_at"])
            df["created_at"] = df["created_at"].dt.tz_convert("Asia/Bangkok").dt.strftime("%Y-%m-%d %H:%M:%S")
        
        metadata_normalized = pd.json_normalize(df[metadata[table]])
        df = pd.concat([df.drop(columns=[metadata[table]]).reset_index(drop=True), metadata_normalized.reset_index(drop=True)], axis=1)

    # pandas map/ apply

    print('-'*20)
    print("cleaned and transformed DataFrame:")
    print('-'*20)
    print(df.head(5))

    os.makedirs(f"data/ecommerce/bronze/{table}", exist_ok=True)
    df.to_parquet(f"data/ecommerce/bronze/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet", index=False)

