import duckdb

def q1_duckdb(db_table_name, print_flag=False):
    sql_request = f"SELECT vendorid, COUNT(*) FROM {db_table_name} GROUP BY 1;"
    data = duckdb.sql(sql_request).fetchall()

    if print_flag:
        data.show()

def q2_duckdb(db_table_name, print_flag=False):
    sql_request = f"""SELECT passenger_count, avg(total_amount)
                  FROM {db_table_name} GROUP BY 1;"""
    data = duckdb.sql(sql_request).fetchall()

    if print_flag:
        data.show()


def q3_duckdb(db_table_name, print_flag=False):
    sql_request = f"""SELECT passenger_count,
                        extract(year from tpep_pickup_datetime),
                        COUNT(*)
                        FROM {db_table_name}
                        GROUP BY 1, 2;"""
    data = duckdb.sql(sql_request).fetchall()

    if print_flag:
        data.show()


def q4_duckdb(db_table_name, print_flag=False):
    sql_request = f"""SELECT
                   passenger_count,
                   extract(year from tpep_pickup_datetime),
                   round(trip_distance),
                   count(*)
                   FROM {db_table_name}
                   GROUP BY 1, 2, 3
                   ORDER BY 2, 4 DESC"""
    data = duckdb.sql(sql_request).fetchall()

    if print_flag:
        data.show()
