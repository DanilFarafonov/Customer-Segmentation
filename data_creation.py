import pandas as pd
from btyd.utils import summary_data_from_transaction_data
from sklearn.model_selection import train_test_split

# читаем файл с транзакционными данными с помощью pandas
df_raw = pd.read_parquet("data/wallet_urfu.parquet.gzip")

# перевод транзакционной истории в сводные данные
df_summary = summary_data_from_transaction_data(transactions=df_raw, customer_id_col="partner",
                                                datetime_col="rep_date", monetary_value_col="monetary")

# разбивка на обучающую и тестовую выборки
df_train, df_test = train_test_split(df_summary, test_size=0.3, random_state=42)

# сохранение датафреймов в csv
df_summary.to_csv("data/data_summary.csv", encoding="utf-8", index="partner")
df_train.to_csv("data/data_train.csv", encoding="utf-8", index="partner")
df_test.to_csv("data/data_test.csv", encoding="utf-8", index="partner")
