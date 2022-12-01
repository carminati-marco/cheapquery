import datetime
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as da
import pyarrow.parquet as pq

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


def _get_table(file_info):
    """
    Get the data
    :param file_info:
    :return:
    """
    _partitioning = da.partitioning(schema=pa.schema([pa.field("publisher_id", pa.int64()),
                                                      pa.field("publisher_domain_id", pa.int64())
                                                      ]),
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
                          filters=[(DF_COLUMNS.PUBLISHER_ID, '=', publisher_id),
                                   (DF_COLUMNS.PUBLISHER_DOMAIN_ID, 'in', publisher_domain_ids),
                                   (DF_COLUMNS.TRANSACTION_DATE, '>=', from_transaction_date),
                                   (DF_COLUMNS.TRANSACTION_DATE, '<', to_transaction_date)
                                   ],
                          filesystem=gcs
                          ).to_pandas()
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
    df = get_df(uri="data-events-test/cheapquery/parquet_partitioning")
    # df = get_df(uri="data-events-test/cheapquery/parquet/enriched-comms-74968-60-days.parquet", is_file=True)
    print(df.count())
    df_1 = apply_group(df)
    df_1.to_csv("result.csv")
    print(df_1.count())
    print("[END] main", datetime.datetime.now())
