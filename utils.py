import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
from dask import dataframe as dd

from arrow_v1 import DF_COLUMNS


def partition_data(df):
    _ddexport = dd.from_pandas(df, npartitions=1)
    _ddexport.to_parquet(
        path="./partition",
        engine='pyarrow',
        compression='snappy',
        partition_on=[DF_COLUMNS.PUBLISHER_ID, DF_COLUMNS.PUBLISHER_DOMAIN_ID]
    )


def convert_files_to_parquet(uri="data-events-test/cheapquery/csv"):
    """
    utils. lists a directory in GCS and save the csv file in parquet format.
    :param uri:
    """
    gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))
    file_list = gcs.get_file_info(pa.fs.FileSelector(uri, recursive=True))
    print(datetime.datetime.now())
    for f in file_list:
        print("[INIT]", datetime.datetime.now())
        df = pd.read_csv(f"gs://{f.path}")
        print(datetime.datetime.now())
        df.to_parquet(f.base_name.replace(".csv", ".parquet"))
        print("[END]", datetime.datetime.now())
    print(datetime.datetime.now())
