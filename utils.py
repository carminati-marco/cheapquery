import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
from dask import dataframe as dd

from arrow_v1 import DF_COLUMNS, get_df

schema = dict(
    id=pa.binary(),
    publisher_id=pa.int64(),
    publisher_domain_id=pa.int64(),
    publisher_domain=pa.string(),
    merchant_name=pa.string(),
    merchant_id=pa.int64(),
    advertiser_name=pa.string(),
    advertiser_id=pa.int64(),
    normalized_target_url=pa.string(),
    normalized_page_url=pa.string(),
    target_url=pa.string(),
    page_url=pa.string(),
    custom_id=pa.string(),
    transaction_date=pa.timestamp("ns"),
    click_date=pa.timestamp("ns"),
    device_type=pa.string(),
    os_family=pa.string(),
    browser_family=pa.string(),
    user_ip_country=pa.string(),
    raw_page_url=pa.string(),
    raw_link_url=pa.string(),
    page_utm_source=pa.string(),
    page_utm_term=pa.string(),
    page_utm_campaign=pa.string(),
    page_utm_medium=pa.string(),
    page_utm_content=pa.string(),
    page_utm_brand=pa.string(),
    link_utm_source=pa.string(),
    link_utm_term=pa.string(),
    link_utm_campaign=pa.string(),
    link_utm_medium=pa.string(),
    link_utm_content=pa.string(),
    fbclid=pa.string(),
    cid=pa.string(),
    ncid=pa.string(),
    source=pa.string(),
    src=pa.string(),
    page_referrer=pa.string(),
    platform_id=pa.int64(),
    publisher_commission_amount=pa.decimal128(14, 9),
    order_amount=pa.decimal128(15, 9),
    sales_count_total=pa.int64(),
    __null_dask_index__=pa.int64(),
    # year=pa.int64(),
    # month=pa.int64(),
    day=pa.int64()
)


def partition_data(df):
    # df["year"] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE]).dt.year
    # df["month"] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE]).dt.month
    # df["day"] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE]).dt.day
    df["day"] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE]).dt.strftime('%Y%m%d').astype(int)

    _ddexport = dd.from_pandas(df, npartitions=1)
    _ddexport.to_parquet(
        schema=schema,
        path="./partition_demo_2",
        engine='pyarrow',
        compression='snappy',
        # partition_on=[DF_COLUMNS.PUBLISHER_ID, "year", "month", "day"]
        partition_on=[DF_COLUMNS.PUBLISHER_ID, "day"]

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


if __name__ == '__main__':
    df = get_df(
        uri="data-events-test/cheapquery/parquet/full-90-day-dataset/gzip/enriched-comms-multi-90-days-gzip000000000000.parquet")
    partition_data(df)
