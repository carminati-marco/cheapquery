import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis

publisher_domain_ids = [1525074, 1525073, 1597725, 1525077]
publisher_id = 74968
from_transaction_date = "2022-11-03"
to_transaction_date = "2022-11-04"

class DF_COLUMNS:
    PUBLISHER_ID = "publisher_id"
    PUBLISHER_DOMAIN_ID = "publisher_domain_id"
    TRANSACTION_DATE = "transaction_date"
    ORDER_AMOUNT = "order_amount"
    PUBLISHER_COMMISSION_AMOUNT = "publisher_commission_amount"
    SALES_COUNT_TOTAL = "sales_count_total"


def _get_table(file_info, redis_connector):
    """
    Get the data
    :param file_info:
    :param redis_connector:
    :return:
    """
    context = pa.default_serialization_context()
    print("[INIT] _get_table", file_info.base_name, datetime.datetime.now())
    key = file_info.base_name
    try:
        table = context.deserialize(redis_connector.get(key))
        print("[END][REDIS] _get_table", file_info.base_name, datetime.datetime.now())
    except TypeError:
        gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))
        table = pq.read_table(file_info.path,
                              columns=[DF_COLUMNS.PUBLISHER_ID,
                                       DF_COLUMNS.PUBLISHER_DOMAIN_ID,
                                       DF_COLUMNS.ORDER_AMOUNT,
                                       DF_COLUMNS.TRANSACTION_DATE,
                                       DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                       DF_COLUMNS.SALES_COUNT_TOTAL],
                              filters=[(DF_COLUMNS.PUBLISHER_ID, '=', publisher_id),
                                       (DF_COLUMNS.PUBLISHER_DOMAIN_ID, 'in', publisher_domain_ids)],
                              filesystem=gcs).to_pandas()
        redis_connector.set(key, context.serialize(table).to_buffer().to_pybytes())
        print("[END][NO REDIS] _get_table", file_info.base_name, datetime.datetime.now())

    return table


def get_df(uri, is_file=True):
    """
    Obtains the df, reading the uri.
    :param uri:
    :param is_file: false if the "uri" directory must be processed.
    :return:
    """
    print("[INIT] get_df", datetime.datetime.now())
    redis_connector = redis.Redis(host='localhost', port=6379, db=0)
    redis_connector.flushdb() # TO CLEAN REDIS CACHE.
    gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))

    file_list = [pa.fs.FileInfo(uri)] if is_file else gcs.get_file_info(pa.fs.FileSelector(uri, recursive=False))
    frames = []
    for _file in file_list:
        frames.append(_get_table(_file, redis_connector))
    print("[END] get_df", datetime.datetime.now())
    return pd.concat(frames)

    # TODO: INTEGRATE async in _get_table
    # event_loop = asyncio.get_event_loop()
    # asyncio.set_event_loop(event_loop)
    # futures = []
    # for _file in file_list:
    #     futures.append(event_loop.create_task(_get_table(_file, redis_connector)))
    # d = event_loop.run_until_complete(asyncio.gather(*futures))
    # event_loop.close()
    # # print("end get_df", datetime.datetime.now())
    # return d

    # for f in file_list:
    #     print("init", datetime.datetime.now())
    #     df = pd.read_csv(f"gs://{f.path}")
    #     print(datetime.datetime.now())
    #     df.to_parquet(f.base_name.replace(".csv", ".parquet"))
    #     print("end", datetime.datetime.now())

    # print(datetime.datetime.now())
    # df = pd.read_csv('gs://data-events-test/cheapquery/dataset/90-day-enriched-commissions000000000001.csv')
    # print(datetime.datetime.now())
    # df.to_parquet("90-day-enriched-commissions000000000001.parquet")
    #
    # gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))
    #
    # # List all contents in a bucket, recursively
    # uri = "data-events-test/cheapquery/dataset/arrow"
    # file_list = gcs.get_file_info(pa.fs.FileSelector(uri, recursive=True))
    #
    # with pa.memory_map('bigfile.arrow', 'rb') as source:
    #     with pa.ipc.open_file(source) as reader:
    #         df = reader.read_pandas()
    #         print(df)


def apply_query(df):
    """
    Function to filter/group/sup the dataframe df
    """
    # TODO: extract the filter as params (to evaluate).

    df = df[(df[DF_COLUMNS.PUBLISHER_ID] == publisher_id) &
            (df[DF_COLUMNS.PUBLISHER_DOMAIN_ID].isin(publisher_domain_ids)) &
            (df[DF_COLUMNS.TRANSACTION_DATE] >= from_transaction_date) &
            (df[DF_COLUMNS.TRANSACTION_DATE] < to_transaction_date)]

    df[DF_COLUMNS.TRANSACTION_DATE] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE])
    df = df.groupby([df[DF_COLUMNS.TRANSACTION_DATE].dt.date])[DF_COLUMNS.ORDER_AMOUNT,
                                                               DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                                               DF_COLUMNS.SALES_COUNT_TOTAL].sum()
    return df


if __name__ == '__main__':
    print("[INIT] main", datetime.datetime.now())
    # df = get_df(uri="data-events-test/cheapquery/parquet", is_file=False)
    df = get_df(uri="data-events-test/cheapquery/parquet/enriched-comms-74968-60-days.parquet", is_file=True)
    print(df.count())
    df_1 = apply_query(df)
    df_1.to_csv("result.csv")
    print(df.count())
    print("[END] main", datetime.datetime.now())


#### UTILS.
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
