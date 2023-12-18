import pandas as pd
from sqlalchemy import create_engine
from csv_to_database import postgres_data, db_table_name
database = f"postgresql://{postgres_data['user=']}:{postgres_data['password=']}@" \
           f"{postgres_data['host=']}:{postgres_data['port=']}/{postgres_data['name=']}"

def q1_pandas(dataframe, print_flag=False):
    data = dataframe[['VendorID']].groupby('VendorID').size().reset_index(name='counts')
    if print_flag:
        print(data)

def q2_pandas(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'total_amount']].groupby('passenger_count').mean().reset_index()
    if print_flag:
        print(data)

def q3_pandas(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'tpep_pickup_datetime']]
    data['year'] = data.loc[:, 'tpep_pickup_datetime'].dt.year
    data = data.groupby(['passenger_count', 'year']).size().reset_index(name='counts')
    if print_flag:
        print(data)

def q4_pandas(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'tpep_pickup_datetime', 'trip_distance']]
    data['trip_distance'] = data['trip_distance'].round().astype(int)
    data["year"] = data.loc[:, "tpep_pickup_datetime"].dt.year
    data = data.groupby(['passenger_count', 'year', 'trip_distance']).size().reset_index(name='counts').sort_values(
        ['year', 'counts'], ascending=[True, False]
    )
    if print_flag:
        print(data)


if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    engine = create_engine(database)
    sql = f"SELECT * FROM {db_table_name}"
    df = pd.read_sql(sql, con=engine)

    q4_pandas(df, True)

    engine.dispose()