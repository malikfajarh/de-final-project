def load_table(table, **kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import gzip

    data_interval_start = kwargs['data_interval_start']
    data_interval_end   = kwargs['data_interval_end']
    ingestion_mode      = kwargs["params"][table]

    csv_path = f"data/ecommerce/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.csv"

    postgres_hook = PostgresHook(postgres_conn_id="postgres_dw")
    conn = postgres_hook.get_conn() 
    cursor = conn.cursor()

    if ingestion_mode == "full" and table == "product":
        query = """
            DO $$ 
            DECLARE 
                tbl RECORD;
            BEGIN
                FOR tbl IN (SELECT table_name FROM information_schema.tables 
                            WHERE table_schema = 'bronze' 
                            AND table_name IN ('transaction', 'product', 'customer'))
                LOOP
                    EXECUTE FORMAT('TRUNCATE TABLE bronze.%I CASCADE', tbl.table_name);
                END LOOP;
            END $$;
        """
        cursor.execute(query)
        
    with gzip.open(csv_path, 'rt') as f:  
        cursor.copy_expert(f"COPY bronze.{table} FROM STDIN WITH CSV HEADER", f)

    conn.commit()
    cursor.close()
    conn.close()
    print(f'{table} data successfully loaded to PostgreSQL with {ingestion_mode} mode')



