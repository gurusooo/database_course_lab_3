import sqlalchemy


def q1_sqlalchemy(connection, db_table_name, print_flag=False):
    sql_request = sqlalchemy.sql.text(f"SELECT \"VendorID\", count(*) FROM {db_table_name} GROUP BY 1;")
    data = connection.execute(sql_request)

    if print_flag:
        for item in data:
            print(item)


def q2_sqlalchemy(connection, db_table_name, print_flag=False):
    sql_request = sqlalchemy.sql.text(f"SELECT passenger_count, avg(total_amount) FROM {db_table_name} GROUP BY 1;")
    data = connection.execute(sql_request)

    if print_flag:
        for item in data:
            print(item)


def q3_sqlalchemy(connection, db_table_name, print_flag=False):
    sql_request = sqlalchemy.sql.text(f"""SELECT passenger_count,
                    extract(year from tpep_pickup_datetime),
                    COUNT(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2;""")
    data = connection.execute(sql_request)

    if print_flag:
        for item in data:
            print(item)


def q4_sqlalchemy(connection, db_table_name, print_flag=False):
    sql_request = sqlalchemy.sql.text(f"""SELECT
                passenger_count,
                extract(year from tpep_pickup_datetime),
                round(trip_distance),
                count(*)
                FROM {db_table_name}
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 DESC""")
    data = connection.execute(sql_request)

    if print_flag:
        for item in data:
            print(item)
