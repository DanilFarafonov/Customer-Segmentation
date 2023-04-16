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


def get_transaction_alive_dead(year: int, month: int, inactivity_days: int, df_transaction: pd.DataFrame):
    """
    Returns dataframes with information about transactions of active("alive") and inactive("dead") customers
    :param year: int,
        end year of the billing period
    :param month: int,
        end year of the billing period
    :param inactivity_days: int,
        the maximum number of days without transactions after which a customer is considered dead
    :param df_transaction: pd.DataFrame,
        dataframe contains transaction history for the entire period
    :rtype: pandas.DataFrame
    """
    df_dated = get_transaction_history(year, month, df_transaction)
    date_end = get_end_of_period_date(year, month)
    df_last_transactions = df_dated.groupby(['partner'], as_index=False).agg({'rep_date':'max'})
    df_last_transactions['is_alive'] = (date_end - df_last_transactions['rep_date']) < timedelta(inactivity_days)
    partners_alive = df_last_transactions[df_last_transactions['is_alive'] == True]['partner']
    partners_dead = df_last_transactions[df_last_transactions['is_alive'] == False]['partner']
    df_alive = df_dated[df_dated['partner'].isin(partners_alive)]
    df_dead = df_dated[df_dated['partner'].isin(partners_dead)]
    return df_alive, df_dead


def get_rfm_data(year: int, month: int, inactivity_days: int, df_transaction: pd.DataFrame):
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
    df_alive, df_dead = get_transaction_alive_dead(year, month, inactivity_days, df_transaction)
    if not df_alive.empty:
        df_rfm_alive = summary_data_from_transaction_data(transactions=df_alive, customer_id_col='partner',
                                                      datetime_col='rep_date', monetary_value_col='monetary')
    else:
        df_rfm_alive = None
    if not df_dead.empty:
        df_rfm_dead = summary_data_from_transaction_data(transactions=df_dead, customer_id_col='partner',
                                                     datetime_col='rep_date', monetary_value_col='monetary')
    else:
        df_rfm_dead = None
    return df_rfm_alive, df_rfm_dead
