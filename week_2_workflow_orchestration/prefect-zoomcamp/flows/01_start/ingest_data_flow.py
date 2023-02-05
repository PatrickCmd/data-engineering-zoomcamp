#!/usr/bin/env python
# coding: utf-8
import argparse
import os
from datetime import timedelta
from time import time
from typing import List

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine


@task(
    log_prints=True,
    retries=3,
    tags=["extract"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url: str):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    os.system(f"wget {url} -O ./data/{csv_name}")

    csv_file = f"data/{csv_name}"
    cwd = os.getcwd()
    csv_file_path = os.path.join(cwd, csv_file)
    print(f"Data file path: {csv_file_path}")

    data_frames = []

    df_iter = pd.read_csv(csv_file_path, iterator=True, chunksize=100000)
    while True:
        try:
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            data_frames.append(df)
        except StopIteration:
            break

    return data_frames


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def load_data(table_name: str, frames: List[pd.DataFrame]):

    connection_block = SqlAlchemyConnector.load("postgres-connector-docker")
    with connection_block.get_connection(begin=False) as engine:
        t_start = time()

        df = frames[0]
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()
        print("inserted 100000 chunk, took %.3f second" % (t_end - t_start))

        for df in frames[1:]:
            t_start = time()

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()
            print("inserted another 100000 chunk, took %.3f second" % (t_end - t_start))

        print("Finished ingesting data into the postgres database")


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Data")
def main_flow(csv_url: str, table_name: str = "yellow_taxi_trips_data"):
    log_subflow(table_name)
    data_frames = extract_data(csv_url)
    data_frames = [transform_data(df) for df in data_frames]
    load_data(table_name, data_frames)


if __name__ == "__main__":
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    main_flow(csv_url=csv_url, table_name="yellow_taxi_trips_data")
