import duckdb
from csv_to_database import db_table_name
database = f"../data/converted/{db_table_name}.db"

def q1_duckdb(print_flag=False):
    sql_request = f"SELECT vendorid, COUNT(*) FROM {db_table_name} GROUP BY 1;"
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()

def q2_duckdb(print_flag=False):
    sql_request = f"""SELECT passenger_count, avg(cast(total_amount as FLOAT))
                  FROM {db_table_name} GROUP BY 1;"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


def q3_duckdb(print_flag=False):
    sql_request = f"""SELECT passenger_count,
                        extract(year from cast(tpep_pickup_datetime as timestamp)),
                        COUNT(*)
                        FROM {db_table_name}
                        GROUP BY 1, 2;"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


def q4_duckdb(print_flag=False):
    sql_request = f"""SELECT
                   passenger_count,
                   extract(year from cast(tpep_pickup_datetime as timestamp)),
                   round(cast(trip_distance as float)),
                   count(*)
                   FROM {db_table_name}
                   GROUP BY 1, 2, 3
                   ORDER BY 2, 4 DESC"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


if __name__ == "__main__":
    # required to run duckdb.sql("INSTALL sqlite") once for sqlite extension for duckdb
    duckdb.sql("SET GLOBAL sqlite_all_varchar=true;")
    duckdb.sql("LOAD sqlite")
    duckdb.sql(f"CALL sqlite_attach('{database}')")
    q4_duckdb()

