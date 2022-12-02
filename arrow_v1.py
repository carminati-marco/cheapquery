import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as da
import pyarrow.parquet as pq

class DF_COLUMNS:
    PUBLISHER_ID = "publisher_id"
    PUBLISHER_DOMAIN_ID = "publisher_domain_id"
    TRANSACTION_DATE = "transaction_date"
    ORDER_AMOUNT = "order_amount"
    PUBLISHER_COMMISSION_AMOUNT = "publisher_commission_amount"
    SALES_COUNT_TOTAL = "sales_count_total"
    DAY = "day"


def get_table(publisher_id, publisher_domain_ids, from_transaction_date, to_transaction_date):
    """
    Get the data
    :param file_info:
    :return:
    """
    file_info = pa.fs.FileInfo(f"data-events-test/cheapquery/partition_demo_publisher/publisher_id={publisher_id}")

    _partitioning = da.partitioning(schema=pa.schema([pa.field(DF_COLUMNS.PUBLISHER_ID, pa.int64())]),
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
                                   DF_COLUMNS.SALES_COUNT_TOTAL,
                                   DF_COLUMNS.DAY],
                          filters=[
                              (DF_COLUMNS.PUBLISHER_DOMAIN_ID, 'in', publisher_domain_ids),
                              (DF_COLUMNS.DAY, '>=', from_transaction_date),
                              (DF_COLUMNS.DAY, '<', to_transaction_date)
                          ],
                          filesystem=gcs
                          ).to_pandas()
    print("[END] _get_table", file_info.base_name, datetime.datetime.now())
    return table



def apply_group(df):
    """
    Function to group/sum the dataframe df
    """
    df[DF_COLUMNS.TRANSACTION_DATE] = pd.to_datetime(df[DF_COLUMNS.TRANSACTION_DATE])
    return df.groupby([df[DF_COLUMNS.TRANSACTION_DATE].dt.date])[DF_COLUMNS.ORDER_AMOUNT,
                                                                 DF_COLUMNS.PUBLISHER_COMMISSION_AMOUNT,
                                                                 DF_COLUMNS.SALES_COUNT_TOTAL].sum()


def search(publisher_id, from_transaction_date, to_transaction_date, publisher_domain_ids):
    df = get_table(publisher_id=publisher_id,
                   from_transaction_date=from_transaction_date,
                   to_transaction_date=to_transaction_date,
                   publisher_domain_ids=publisher_domain_ids)
    print(df.count())
    grouped_df = apply_group(df)
    # grouped_df.to_csv("result.csv")
    # print(grouped_df.count())
    print("[END] main", datetime.datetime.now())
    return grouped_df


if __name__ == '__main__':
    print("[INIT] main", datetime.datetime.now())

    publisher_domain_ids = [1705172, 1552619, 1701740, 1701743, 1571397, 1599780]
    publisher_id = 74968
    from_transaction_date = 20211203
    to_transaction_date = 20221204

    search(publisher_id, from_transaction_date, to_transaction_date, publisher_domain_ids)