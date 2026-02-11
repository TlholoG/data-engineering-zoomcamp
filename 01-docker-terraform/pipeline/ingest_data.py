#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
    
yellow_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

yellow_parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

zones_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

# GENERIC INGEST FUNCTION
def ingest_csv(
        url,
        engine,
        target_table,
        dtype=None,
        parse_dates=None,
        chunksize=None
):
    if chunksize:
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
        )

        first = True
        for df_chunk in tqdm(df_iter,f'Ingesting data into {target_table}'):
            if first:
                df_chunk.head(0).to_sql(
                    name=target_table, 
                    con=engine, 
                    if_exists='replace'
                )
                first = False

        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
        )
    else:
        df = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates
        )

        df.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='replace'
        )
        
# CLI

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option(
    '--target-table', 
    type=click.Choice(['yellow_taxi_data', 'zones'], case_sensitive=False), 
    required=True, 
    help='Database table to ingest data into')
@click.option('--year', default=2021, type=int, help='Year of data to ingest')
@click.option('--month', default=1, type=int, help='Month of data to ingest')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month, chunksize):
   
    

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    if target_table == 'yellow_taxi_data':
        prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
        url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'

        ingest_csv(
            url=url,
            engine=engine,
            target_table=target_table,
            dtype=yellow_dtype,
            parse_dates=yellow_parse_dates,
            chunksize=chunksize
        )

    elif target_table == 'zones':
        url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

        ingest_csv(
            url=url,
            engine=engine,
            target_table=target_table,
            dtype=zones_dtype,
            chunksize=None
        )



if __name__ == '__main__':
    run()

