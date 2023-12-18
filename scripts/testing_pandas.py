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
