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


def RScore(x, p, d):
    if x <= d[p][0.20]:
        return 5
    elif x <= d[p][0.40]:
        return 4
    elif x <= d[p][0.60]:
        return 3
    elif x <= d[p][0.80]:
        return 2
    else:
        return 1


def FnMScoring(x, p, d):
    if x <= d[p][0.20]:
        return 1
    elif x <= d[p][0.40]:
        return 2
    elif x <= d[p][0.60]:
        return 3
    elif x <= d[p][0.80]:
        return 4
    else:
        return 5


def get_rfm_cluster_alive(df):
  df['R'] = df['recency'].apply(RScore, args=('recency',quantiles,))
  df['F'] = df['frequency'].apply(FnMScoring, args=('frequency',quantiles,))
  df['M'] = df['monetary_value'].apply(FnMScoring, args=('monetary_value',quantiles,))
  df['RFMScore'] = df.R.map(str) + df.F.map(str) + df.M.map(str)
  return df


def get_rfm_cluster_dead(df):
  df['R'] = "0"
  df['F'] = "0"
  df['M'] = "0"
  df['RFMScore'] = "000"
  return df


def set_cluster_categoty(df):
  seg_map = {
      r'00': '0',
      r'[1-2][1-2]': '1',
      r'[1-2][3-4]': '2',
      r'[1-2]5': '3',
      r'3[1-2]': '4',
      r'33': '5',
      r'[3-4][4-5]': '6',
      r'41': '7',
      r'51': '8',
      r'[4-5][2-3]': '9',
      r'5[4-5]': '10'
  }
  df['Cluster'] = df['R'].astype(str) + df['F'].astype(str)
  df['Cluster'] = df['Cluster'].replace(seg_map, regex=True)
  return df


df = pd.read_parquet("wallet_urfu.parquet.gzip")
df_final = pd.DataFrame()

for year in range(2022, 2023):
    for month in range(1, 13):
        if year == 2023:
            if month == 3:
                break

        df_alive, df_dead = get_rfm_data(year, month, 95, df)

        quantiles = df_alive.quantile(q=[0.20, 0.40, 0.60, 0.80])
        quantiles = quantiles.to_dict()

        df_alive = get_rfm_cluster_alive(df_alive)
        df_dead = get_rfm_cluster_dead(df_dead)

        df_sum = df_alive.append(df_dead)
        df_sum = set_cluster_categoty(df_sum)

        df_sum = df_sum.groupby('Cluster').agg({'monetary_value': ['sum', 'count']})
        df_sum['year'] = year
        df_sum['month'] = month
        df_final = df_final.append(df_sum)

        print(f'''{datetime.now()} Год {year}, месяц {month} окончен''')


df_final = df_final.dropna()
df_final = df_final.rename(columns={'monetary_value.1': 'partner_count', 'Unnamed: 0': 'Cluster'})
seg_map_names = {
      '0': 'Ушедшие',
      '1': 'Бездействующие',
      '2': 'Зона риска',
      '3': 'Не должны потерять',
      '4': 'Засыпающие',
      '5': 'Нуждающиеся во внимании',
      '6': 'Лояльные клиенты',
      '7': 'Перспективные клиенты',
      '8': 'Новые клиенты',
      '9': 'Потенциально лояльные',
      '10': 'VIP'
  }
df_final['Cluster_name'] = df_final['Cluster'].replace(seg_map_names)
df_final.to_csv('data_clustered_by_month.csv', encoding='utf-8', index=False)
