import argparse

import pandas as pd

import pyoxr
from pyoxr import OXRClient
import requests
from pyoxr import OXRInvalidBaseError

from sqlalchemy import create_engine, exc

db_config = {
    'user': 'admin',
    'password': '123456789',
    'host': '100.00.000.000',
    'port': '12345',
    'database': 'database'
}

events_file_columns = [
    'Event Date', 'App Apple ID',
    'Subscription Apple ID', 'Introductory Price Type',
    'Introductory Price Duration', 'Marketing Opt-In Duration',
    'Subscriber ID', 'Customer Price',
    'Developer Proceeds', 'Preserved Pricing',
    'Proceeds Reason', 'Client',
    'Device', 'Country',
    'Subscriber ID Reset', 'Refund',
    'Purchase Date', 'Units'
]

events_bool_type_columns = ['Preserved Pricing', 'Proceeds Reason',
                            'Client', 'Subscriber ID Reset', 'Refund']

events_date_type_columns = ['Event Date', 'Purchase Date']

events_table_columns = [
    'date', 'app_apple_id',
    'subscription_apple_id', 
    'introductory_price_type',
    'introductory_price_duration', 'marketing_opt_in_duration',
    'subscriber_id', 'customer_price',
    'developer_proceeds', 'is_price_preserved',
    'is_proceed_higher', 'is_purchased_from_news',
    'device', 'country_code',
    'is_subscriber_id_reset', 'is_refund',
    'purchase_date', 'units'
]

apps_file_columns = ['App Apple ID', 'App Name']

apps_table_columns = ['id', 'name']

subscriptions_file_columns = [
    'Subscription Apple ID', 'Subscription Name',
    'Subscription Duration', 'Subscription Group ID'
]

subscriptions_table_columns = ['id', 'name', 'duration', 'group_id']

country_to_currency_file_columns = ['Country', 'Customer Currency']

country_to_currency_table_columns = ['country_code', 'currency_code']

exchange_rates_table_columns = [
    'exchange_date', 
    'currency_code', 
    'usd_to_currency_code'
]


def get_exchange_rates(date, currencies_to_sync):
    try:
        exchange_rates_by_date = OXRClient.get_historical(
            date=date, symbols=currencies_to_sync
        )
    except OXRInvalidBaseError:
        raise Exception("exchange rate")
    return pd.DataFrame([{'exchange_date': date,
                          'currency_code': k,
                          'usd_to_currency_code': float(v)}
                         for k, v in exchange_rates_by_date["rates"].items()])


def insert_df_to_mysql_table(df, table, db_engine):
    num_rows = len(df)

    for i in range(num_rows):
        try:
            df.iloc[i:i + 1].to_sql(
                name=table, 
                con=db_engine, 
                if_exists='append', 
                index=False
            )
        except exc.IntegrityError:
            # Ignore duplicates_of_rows
            pass


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='import report file to database')
    parser.add_argument('report_dir', type=str, help='dir of report file')
    args = parser.parse_args()

    try:
        df = pd.read_csv(args.report_dir, sep='	')
    except Exception as e:
        print("Error reading csv: {}".format(e))

    events_df = df.loc[:, events_file_columns].copy(deep=True)
    apps_df = df.loc[:, apps_file_columns].drop_duplicates(keep='first', inplace=False)
    subscriptions_df = df.loc[:, subscriptions_file_columns].drop_duplicates(
        keep='first', inplace=False
    )
    country_to_currency_df = df.loc[:, country_to_currency_file_columns].drop_duplicates(
        keep='first', inplace=False
    )
    currency_by_day_df = df.loc[:, ['Event Date', 'Customer Currency']].drop_duplicates(
        keep='first', inplace=False
    )

    exchange_rates_df = pd.DataFrame(columns=exchange_rates_table_columns)

    del df

    for col in events_bool_type_columns:
        events_df[col] = events_df[col].notnull()

    for col in events_date_type_columns:
        events_df[col] = pd.to_datetime(events_df[col], format='%Y-%m-%d')

    for date in currency_by_day_df['Event Date'].unique():
        currencies_to_sync = list(
            currency_by_day_df.loc[currency_by_day_df[
                'Event Date'] == date, 'Customer Currency'
            ].unique()
        )
        currencies_by_date_df = get_exchange_rates(date, currency_by_day_df)
        exchange_rates_df = exchange_rates_df.append(
            currencies_by_date_df, 
            sort=False, 
            ignore_index=True
        )

    events_df.rename(
        columns=dict(zip(events_file_columns, events_table_columns)), 
        inplace=True
    )
    apps_df.rename(
        columns=dict(zip(apps_file_columns, apps_table_columns)), 
        inplace=True
    )
    subscriptions_df.rename(
        columns=dict(zip(subscriptions_file_columns, subscriptions_table_columns)), 
        inplace=True
    )
    country_to_currency_df.rename(
        columns=dict(zip(country_to_currency_file_columns, country_to_currency_table_columns)), 
        inplace=True
    )
    db_engine = sqlalchemy.create_engine(
        'mysql://{user}:{password}@{host}:{port}/{database}'.format(**db_config),
        echo=False
    )

    in_which_table_df_dict = {
        'events': events_df,
        'apps': apps_df,
        'subsctiptions': subscriptions_df,
        'country_to_currency': country_to_currency_df,
        'exchange_rates': exchange_rates_df
    }

    try:
        for table, df in table_df_dict.items():
            insert_df_to_mysql_table(df, table, db_engine)
    except Exception as e:
        print("Error inserting data into database: {}".format(e))
