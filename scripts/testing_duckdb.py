import duckdb
from csv_to_database import db_table_name, postgres_data
database = f"postgres://{postgres_data['user=']}:{postgres_data['password=']}@" \
           f"{postgres_data['host=']}:{postgres_data['port=']}/{postgres_data['name=']}"

def q1_duckdb(print_flag=False):
    sql_request = f"SELECT vendorid, COUNT(*) FROM {db_table_name} GROUP BY 1;"
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()

def q2_duckdb(print_flag=False):
    sql_request = f"""SELECT passenger_count, avg(total_amount)
                  FROM {db_table_name} GROUP BY 1;"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


def q3_duckdb(print_flag=False):
    sql_request = f"""SELECT passenger_count,
                        extract(year from tpep_pickup_datetime),
                        COUNT(*)
                        FROM {db_table_name}
                        GROUP BY 1, 2;"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


def q4_duckdb(print_flag=False):
    sql_request = f"""SELECT
                   passenger_count,
                   extract(year from tpep_pickup_datetime),
                   round(trip_distance),
                   count(*)
                   FROM {db_table_name}
                   GROUP BY 1, 2, 3
                   ORDER BY 2, 4 DESC"""
    data = duckdb.sql(sql_request)

    if print_flag:
        data.show()


if __name__ == "__main__":
    # required to run duckdb.sql("INSTALL postgres") once for postgres extension for duckdb
    duckdb.sql(f"CALL postgres_attach('{database}')")
    q4_duckdb(True)
