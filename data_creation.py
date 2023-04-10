import pandas as pd
from btyd.utils import summary_data_from_transaction_data
from datetime import date, timedelta
from calendar import monthrange


def get_end_of_period_date(year: int, month: int):
    """
    Returns the end date of the billing period
    :param year: int, end year of the billing period
    :param month: int, month of the end of the billing period
    :return: datetime.date
    """
    date_current = date(year, month, 1)
    days_in_current_month = monthrange(year, month)[1]
    date_end = date_current + timedelta(days=days_in_current_month)
    return date_end


def get_transaction_data(year: int, month: int):
    """
    Returns transaction history from the beginning of the period up to and including the specified year and month
    :param year: int, end year of the billing period
    :param month: int, month of the end of the billing period
    :return: Pandas Dataframe
    """
    end_of_period = get_end_of_period_date(year, month)
    df_raw = pd.read_parquet("data/wallet_urfu.parquet.gzip")
    df_dated = df_raw[df_raw['rep_date'] < end_of_period]
    return df_dated


def get_rfm_data(year: int, month: int):
    """
    Returns RFM data from the beginning of the period up to and including the specified year and month
    :param year:
    :param month:
    :return:
    """
    df_transactions = get_transaction_data(year, month)
    df_summary = summary_data_from_transaction_data(transactions=df_transactions, customer_id_col="partner",
                                                    datetime_col="rep_date", monetary_value_col="monetary")
    return df_summary
