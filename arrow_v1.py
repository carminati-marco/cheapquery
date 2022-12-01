import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as da
import pyarrow.parquet as pq

# publisher_domain_ids = [1525074, 1525073, 1597725, 1525077]
# publisher_id = 10026

publisher_domain_ids = [1705172, 1552619, 1701740, 1701743, 1571397, 1599780]
publisher_id = 1025

dt = datetime.datetime.now()
from_transaction_date = "2022-11-03"
to_transaction_date = "2022-11-04"


class DF_COLUMNS:
    PUBLISHER_ID = "publisher_id"
    PUBLISHER_DOMAIN_ID = "publisher_domain_id"
    TRANSACTION_DATE = "transaction_date"
    ORDER_AMOUNT = "order_amount"
    PUBLISHER_COMMISSION_AMOUNT = "publisher_commission_amount"
    SALES_COUNT_TOTAL = "sales_count_total"


def _get_table(file_info):
    """
    Get the data
    :param file_info:
    :return:
    """
    _partitioning = da.partitioning(schema=pa.schema([pa.field("publisher_id", pa.int64()),
                                                      pa.field("year", pa.int64()),
                                                      pa.field("month", pa.int64()),
                                                      pa.field("day", pa.int64())
                                                      ]),
                                    dictionaries={
                                        "publisher_id": pa.array([publisher_id]),
                                        "year": pa.array([2022]),
                                        "month": pa.array([11])},
                                    flavor='hive')

    print("[INIT] _get_table", file_info.base_name, datetime.datetime.now())
    gcs = pa.fs.GcsFileSystem(anonymous=False, retry_time_limit=timedelta(seconds=15))
    table = pq.read_table(file_info.path,
                          partitioning=_partitioning,
                          columns=[DF_COLUMNS.PUBLISHER_ID,
                                   DF_COLUMNS.PUBLISHER_DOMAIN_ID,
                                   DF_COLUMNS.ORDER_AMOUNT,
                                   DF_COLUMNS.TRANSACTION_DATE,
                                   DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                   DF_COLUMNS.SALES_COUNT_TOTAL],
                          filters=[
                              # (DF_COLUMNS.PUBLISHER_ID, '=', publisher_id),
                              (DF_COLUMNS.PUBLISHER_DOMAIN_ID, 'in', publisher_domain_ids),
                              (DF_COLUMNS.TRANSACTION_DATE, '>=',
                               pa.scalar(datetime.datetime.strptime(from_transaction_date, "%Y-%m-%d"))),
                              (DF_COLUMNS.TRANSACTION_DATE, '<',
                               pa.scalar(datetime.datetime.strptime(to_transaction_date, "%Y-%m-%d")))
                          ],
                          filesystem=gcs
                          ).to_pandas()
    print("[END] _get_table", file_info.base_name, datetime.datetime.now())
    return table


def get_df(uri):
    """
    Obtains the df, reading the uri.
    :param uri:
    :return:
    """
    print("[INIT] get_df", datetime.datetime.now())
    df = _get_table(pa.fs.FileInfo(uri))
    print("[END] get_df", datetime.datetime.now())
    return df


def apply_group(df):
    """
    Function to group/sum the dataframe df
    """
    df[DF_COLUMNS.TRANSACTION_DATE] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE])
    return df.groupby([df[DF_COLUMNS.TRANSACTION_DATE].dt.date])[DF_COLUMNS.ORDER_AMOUNT,
                                                                 DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                                                 DF_COLUMNS.SALES_COUNT_TOTAL].sum()


if __name__ == '__main__':
    print("[INIT] main", datetime.datetime.now())

    # df = get_df(uri="data-events-test/cheapquery/parquet/enriched-comms-74968-90-days.parquet")
    # df = get_df(uri="data-events-test/cheapquery/partition_demo_client")
    df = get_df(uri=f"data-events-test/cheapquery/partition_demo_client/publisher_id={publisher_id}/year=2022/month=11/day=11")
    # df = get_df(uri="data-events-test/cheapquery/parquet/enriched-comms-74968-60-days.parquet", is_file=True)
    print(df.count())
    df_1 = apply_group(df)
    df_1.to_csv("result.csv")
    print(df_1.count())
    print("[END] main", datetime.datetime.now())
