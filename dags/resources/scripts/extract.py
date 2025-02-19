import pandas as pd
import ast
import re
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


def to_snake_case(string):
    string = re.sub(r'([a-z])([A-Z])', r'\1_\2', string)  
    return string.lower() 

def safe_parse(data):
    if pd.isna(data):
        return {}
    try:
        return ast.literal_eval(data)
    except:
        return {}

def get_last_id(postgres_hook, table, id):
    query = f"""
        SELECT data_type FROM information_schema.columns 
        WHERE table_schema = 'bronze' AND table_name = '{table}' 
        AND column_name = '{id}';
    """
    data_type = postgres_hook.get_first(query)

    if not data_type:
        raise ValueError(f"Column {id} not found in table {table}")

    data_type = data_type[0] 

    if data_type in ("integer"):
        query = f"SELECT COALESCE(MAX({id}), 0) FROM bronze.{table};"
    elif data_type in ("uuid"):
        query = f"SELECT {id}::TEXT FROM bronze.{table} ORDER BY {id} DESC LIMIT 1;"

    last_id = postgres_hook.get_first(query)
    return last_id[0] if last_id and last_id[0] else (0 if data_type in ("integer") else "00000000-0000-0000-0000-000000000000")


def extract_table(table, **kwargs):
    data_interval_start = kwargs['data_interval_start']
    data_interval_end   = kwargs['data_interval_end']
    ingestion_mode      = kwargs["params"][table]

    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dir, '..', 'csv', f'{table}.csv')

    pd.set_option('display.max_columns', None)
    df = pd.read_csv(csv_path)

    df.columns = [to_snake_case(col) for col in df.columns]
    metadata = {'transaction': 'product_metadata'}

    if table in metadata:
        df[metadata[table]] = [safe_parse(item) for item in df[metadata[table]]]
        df = df.explode(metadata[table])    

        df["created_at"] = pd.to_datetime(df["created_at"])
        df["created_at"] = df["created_at"].dt.tz_convert("Asia/Bangkok").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["shipment_date_limit"] = pd.to_datetime(df["shipment_date_limit"])
        df["shipment_date_limit"] = df["shipment_date_limit"].dt.tz_convert("Asia/Bangkok").dt.strftime("%Y-%m-%d %H:%M:%S")

        metadata_normalized = pd.json_normalize(df.pop(metadata[table]))
        df = pd.concat([df.reset_index(drop=True), metadata_normalized.reset_index(drop=True)], axis=1)

    elif table == 'product':
        df['year'] = df['year'].fillna(0).astype(int)

        pattern = r'^(.*?)\s+(?=(?:Mens|Men|Unisex|Boy\'s|Boys|Girl\'s|Girls|Kids|Teen)\b)'
        df['brand'] = df['product_display_name'].str.extract(pattern, flags=re.IGNORECASE, expand=False)
        df['brand'] = df['brand'].fillna(df['product_display_name'].str.split().str[0]).str.strip()
    

    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = postgres_hook.get_conn() 
    cursor = conn.cursor()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(current_dir, '..', 'sql', 'bronze' ,f'{table}.sql')
    with open(sql_path, 'r') as file:
        sql = file.read()

    cursor.execute(sql)
    conn.commit()

    table_ids = {'product': 'id', 'customer': 'customer_id', 'transaction': 'booking_id'}

    if ingestion_mode == "incremental":
        last_id = get_last_id(postgres_hook, table, table_ids[table])
        df = df[df[table_ids[table]] > last_id]
        if last_id == 0:
            print(f"Found table {table} with {table_ids[table]} = {last_id}, performing load new data..")
        else:
            print(f'getting table {table} new data where {table_ids[table]} > {last_id}')

    print('-'*50)
    print("cleaned and transformed DataFrame:")
    print('-'*50)
    print(df.head(5))

    os.makedirs(f"data/ecommerce/{table}", exist_ok=True)
    df.to_csv(f"data/ecommerce/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.csv", index=False, compression="gzip")

