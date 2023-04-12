import pandas as pd
from btyd.utils import summary_data_from_transaction_data
from datetime import date, timedelta
from calendar import monthrange


def get_end_of_period_date(year: int, month: int):
    """
    Returns the end date of the billing period.
    :param year: int, end year of the billing period
    :param month: int, month of the end of the billing period
    :rtype: datetime.date
    """
    date_current = date(year, month, 1)
    days_in_current_month = monthrange(year, month)[1]
    date_end = date_current + timedelta(days=days_in_current_month)
    return date_end


def get_transaction_history(year: int, month: int, df_transaction: pd.DataFrame):
    """
    Returns transaction history from the beginning of the period up to and including the specified year and month.
    :param year: int,
        end year of the billing period
    :param month: int,
        month of the end of the billing period
    :param df_transaction: pd.DataFrame,
        dataframe contains transaction history for the entire period
    :rtype: pandas.DataFrame
    """
    end_of_period = get_end_of_period_date(year, month)
    df_dated = df_transaction[df_transaction['rep_date'] < end_of_period]
    return df_dated


def get_transaction_alive_dead(year: int, month: int, df_transaction: pd.DataFrame):
    pass


def get_rfm_data(year: int, month: int, df_transaction: pd.DataFrame):
    """
    Returns RFM data on two large clusters: alive and dead clients.
    :param year: int,
        end year of the billing period
    :param month: int,
        month of the end of the billing period
    :param df_transaction: pd.DataFrame,
        dataframe contains transaction history for the entire period
    :rtype: tuple
    """
    df_alive, df_dead = get_transaction_alive_dead(year, month, df_transaction)
    df_rfm_alive = summary_data_from_transaction_data(transactions=df_alive, customer_id_col="partner",
                                                      datetime_col="rep_date", monetary_value_col="monetary")
    df_rfm_dead = summary_data_from_transaction_data(transactions=df_dead, customer_id_col="partner",
                                                     datetime_col="rep_date", monetary_value_col="monetary")
    return df_rfm_alive, df_rfm_dead
