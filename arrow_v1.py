import datetime
import sys
from datetime import timedelta
from cachetools import cached, LFUCache
import psutil
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as da
import pyarrow.parquet as pq
import functools
import numpy as np
from helpers import to_datetime, convert_date

class DF_COLUMNS:
    PUBLISHER_ID = "publisher_id"
    PUBLISHER_DOMAIN_ID = "publisher_domain_id"
    TRANSACTION_DATE = "transaction_date"
    ORDER_AMOUNT = "order_amount"
    PUBLISHER_COMMISSION_AMOUNT = "publisher_commission_amount"
    SALES_COUNT_TOTAL = "sales_count_total"
    DAY = "day"

AGG_COLS = [
    DF_COLUMNS.ORDER_AMOUNT,
    DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
    DF_COLUMNS.SALES_COUNT_TOTAL
]


CACHE_LIMIT = psutil.virtual_memory().total / 2
    
class APP_CACHE(LFUCache):
    def __init__(self):
        print(f"Initialised cache with {CACHE_LIMIT / 1024000}MB limit")
        super().__init__(CACHE_LIMIT)

    
    def __getitem__(self, key):
        return super().__getitem__(key)
    
    def __setitem__(self, key, values):
        from_date, to_date, data = values
        obj = dict(
            from_date=from_date,
            to_date=to_date,
            data=data
        )
        print(f"Storing data cache: {sys.getsizeof(data) / 1024000} MB")
        return super().__setitem__(key, obj)
    
    def __missing__(self, key):
        return None



def get_table(publisher_id, publisher_domain_ids, from_transaction_date, to_transaction_date, app_cache={}):
    """
    Get the data
    :param file_info:
    :return:
    """
    from_transaction_date = to_datetime(from_transaction_date)
    to_transaction_date = to_datetime(to_transaction_date) + timedelta(days=1)

    cache_entry = app_cache[publisher_id]
    if cache_entry is not None and from_transaction_date >= cache_entry['from_date'] and to_transaction_date <= cache_entry['to_date']:
        print("Using cached data")
        return apply_filters(cache_entry['data'], from_transaction_date, to_transaction_date, publisher_domain_ids)
    

    file_info = pa.fs.FileInfo(f"data-events-test/cheapquery/partition_demo_publisher/publisher_id={publisher_id}")
    _partitioning = da.partitioning(schema=pa.schema([pa.field(DF_COLUMNS.PUBLISHER_ID, pa.int64()),pa.field(DF_COLUMNS.DAY, pa.int64())]), flavor='hive')

    print("[INIT] Fetching data from GCS", file_info.base_name, datetime.datetime.now())
    gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))
    table = pq.read_table(file_info.path,
                          partitioning=_partitioning,
                          columns=[DF_COLUMNS.PUBLISHER_ID,
                                   DF_COLUMNS.PUBLISHER_DOMAIN_ID,
                                   DF_COLUMNS.ORDER_AMOUNT,
                                   DF_COLUMNS.TRANSACTION_DATE,
                                   DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                   DF_COLUMNS.SALES_COUNT_TOTAL,
                                   DF_COLUMNS.DAY],
                          filters=[(DF_COLUMNS.DAY, '>=', convert_date(from_transaction_date)),
                                    (DF_COLUMNS.DAY, '<', convert_date(to_transaction_date))],
                          filesystem=gcs
                        ).to_pandas()
    print("[END] Fetching data from GCS", file_info.base_name, datetime.datetime.now())

    app_cache[publisher_id] = (from_transaction_date, to_transaction_date, table)
    return apply_filters(table, publisher_domain_ids=publisher_domain_ids)


def apply_filters(df, from_date=None, to_date=None, publisher_domain_ids=None):
    # If the data is cached and we select a subset of the data filter the subset
    def join(*conditions):
        return functools.reduce(np.logical_and, conditions)

    conditions = []

    if from_date:
        conditions += [(df[DF_COLUMNS.TRANSACTION_DATE]>=pd.Timestamp(from_date))]

    if to_date:
        conditions += [(df[DF_COLUMNS.TRANSACTION_DATE]<pd.Timestamp(to_date))]

    if publisher_domain_ids:
        conditions += [(df[DF_COLUMNS.PUBLISHER_DOMAIN_ID].isin(publisher_domain_ids))]

    return df[join(*conditions)] if conditions else df


def apply_group(df, agg_cols=AGG_COLS):
    """
    Function to group/sum the dataframe df
    """
    df[DF_COLUMNS.TRANSACTION_DATE] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE])
    return df.groupby([df[DF_COLUMNS.TRANSACTION_DATE].dt.date])[agg_cols].apply(lambda x: x.sum()).reset_index()


def search(publisher_id, from_transaction_date, to_transaction_date, publisher_domain_ids, app_cache={}):
    print("[INIT] main", datetime.datetime.now())
    df = get_table(publisher_id=publisher_id,
                   from_transaction_date=from_transaction_date,
                   to_transaction_date=to_transaction_date,
                   publisher_domain_ids=publisher_domain_ids, app_cache=app_cache)
    # print(df.count())
    grouped_df = apply_group(df)
    # print(grouped_df)
    # grouped_df.to_csv("result.csv")
    # print(grouped_df.count())
    print("[END] main", datetime.datetime.now())
    return grouped_df


if __name__ == '__main__':
    print("[INIT] main", datetime.datetime.now())

    publisher_domain_ids = [1524972,
    1525071,
    1525072,
    1525073,
    1525074,
    1525076,
    1525077,
    1525078,
    1525079,
    1525080,
    1525081,
    1525082,
    1525083,
    1525084,
    1525085,
    1525086,
    1525087,
    1525088,
    1525089,
    1538880,
    1547195,
    1547755,
    1550682,
    1550683,
    1551729,
    1553576,
    1553877,
    1562278,
    1562435,
    1570127,
    1576254,
    1576255,
    1576256,
    1576257,
    1576258,
    1582515,
    1583755,
    1584043,
    1587893,
    1593542,
    1596630,
    1597725,
    1600402,
    1606475,
    1607820,
    1614486,
    1614487,
    1616970,
    1618092,
    1639501,
    1655324,
    1701363]
    publisher_id = 74968
    from_transaction_date = 20211203
    to_transaction_date = 20221204

    search(publisher_id, from_transaction_date, to_transaction_date, publisher_domain_ids)