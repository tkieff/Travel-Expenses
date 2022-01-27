import pandas as pd
import numpy as np
import itertools
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'travelq.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(url, dtype={'name': str})

    # remove nan values in numeric columns
    df.loc[:, df.dtypes == float] = df.loc[:, df.dtypes == float].fillna(0)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

    # drop duplicated columns
    df.drop(columns=['title_fr', 'purpose_fr', 'destination_fr', 'additional_comments_fr'], inplace=True)

    # use the total column to validate the others.
    df['delta'] = abs(
        df['total'] - df['other_expenses'] - df['meals'] - df['lodging'] - df['other_transport'] - df['airfare'])
    # round
    df = df.round(decimals=2)

    # Sometimes the total is duplicated in the other_expenses column
    # The rows with duplicated total and other_expenses are in a series
    # for now I will drop them, I have contacted the curator of this data.
    df.drop(range(67469, 67487), inplace=True)

    # The others seems like the total just wasn't calculated
    zeros = df['total'] == 0
    df.loc[zeros, 'total'] = df['other_expenses'] + df['meals'] + df['lodging'] + df['other_transport'] + df['airfare']
    df.loc[zeros, 'delta'] = df['total'] - (
            df['other_expenses'] + df['meals'] + df['lodging'] + df['other_transport'] + df['airfare'])

    # There are no values in this dataset above $20000 that seem correct
    # for every value above $20000 I'm going to recalculate the value based off the other informatino
    for i, row in df.loc[(df['delta'] > 1000) &
                         ((df['total'] > 20000) |
                          (df['other_expenses'] > 20000) |
                          (df['meals'] > 17000) |
                          (df['lodging'] > 20000) |
                          (df['other_transport'] > 20000) |
                          (df['airfare'] > 20000))
    ].iterrows():
        for j, item in row.iteritems():
            try:
                if item > 17000 and j == 'total':
                    df.at[i, j] = row['other_expenses'] + row['meals'] + row['lodging'] + row['other_transport'] + row[
                        'airfare']
                    df.at[i, 'delta'] = abs(
                        df.at[i, 'total'] - df.at[i, 'other_expenses'] - df.at[i, 'meals'] - df.at[i, 'lodging'] -
                        df.at[
                            i, 'other_transport'] - df.at[i, 'airfare'])
                elif item > 17000 and (j != 'total' or j != 'delta'):
                    df.at[i, j] = row[j] + row['total'] - row['other_expenses'] - row['meals'] - row['lodging'] - row[
                        'other_transport'] - row['airfare']
                    df.at[i, 'delta'] = abs(
                        df.at[i, 'total'] - df.at[i, 'other_expenses'] - df.at[i, 'meals'] - df.at[i, 'lodging'] -
                        df.at[
                            i, 'other_transport'] - df.at[i, 'airfare'])
            except:
                pass
    # fixed the most extreme outliers but now the data seems to get even more messy
    # some of these entries they just don't include airfare in the total
    df.loc[(df['delta'] == df['airfare']) & (df['delta'] > 1), 'total'] = df['total'] + df['airfare']
    df.loc[(df['delta'] == df['airfare']) & (df['delta'] > 1), 'delta'] = abs(
        df['total'] - df['other_expenses'] - df['meals'] - df['lodging'] - df['other_transport'] - df['airfare'])

    # some entries are close to the airfare or have multiples of airfare
    df.loc[(df['delta'] % df['airfare'] == 0) & (df['delta'] > 1), 'total'] = df['total'] - df['delta']
    df.loc[(df['delta'] == 2 * df['airfare']) & (df['delta'] > 1), 'delta'] = abs(
        df['total'] - df['other_expenses'] - df['meals'] - df['lodging'] - df['other_transport'] - df['airfare'])

    def totalChecker(row):
        for i in range(5):
            for j in itertools.combinations(
                    list(row[['other_expenses', 'meals', 'lodging', 'other_transport', 'airfare']]),
                    i):
                if np.sum(j) > row['delta'] - 1 and np.sum(j) < row['delta'] + 1:
                    return True, row[['other_expenses', 'meals', 'lodging', 'other_transport', 'airfare']].sum()
        return False, None


    for index, row in df.iterrows():
        if row['delta'] > 1:
            changeTotal, newTotal = totalChecker(row)
            if changeTotal == True:
                df.at[index, 'total'] = newTotal
                df.at[index, 'delta'] = 0

    df.drop(60683, inplace=True)

    # Thinking it's time to just recalculate the total, most remaining errors seem to be in calculation of total.
    df.loc[df['delta'] > 1, 'total'] = df['other_expenses'] + df['meals'] + df['lodging'] + df['other_transport'] + df[
        'airfare']
    df.loc[df['delta'] > 1, 'delta'] = 0

    df.to_sql(name=table_name, con=engine, if_exists='replace')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # user
    # password
    # host
    # port
    # database name
    # table name
    # url of the csv
    parser.add_argument('--user', help='Username for Postgres')
    parser.add_argument('--password', help='Password for Postgres')
    parser.add_argument('--host', help='Host for Postgres')
    parser.add_argument('--port', help='Port for Postgres')
    parser.add_argument('--db', help='database name for Postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)




