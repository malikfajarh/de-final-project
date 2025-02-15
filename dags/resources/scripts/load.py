def load_table(table, **kwargs):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from sqlalchemy import text

    data_interval_start = kwargs['data_interval_start']
    data_interval_end   = kwargs['data_interval_end']
    ingestion_mode      = kwargs["params"][table]

    df = pd.read_parquet(f"data/ecommerce/bronze/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet")

    engine = PostgresHook("postgres_dw").get_sqlalchemy_engine()
    with engine.connect() as conn:
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        
        if ingestion_mode == "incremental":
            mode = "append"
        else:
            mode = "replace"

        df.to_sql(table, conn, schema="bronze", index=False, if_exists=mode, method="multi", chunksize=50000)

    print(f'{table} data successfully loaded to postgresql')


