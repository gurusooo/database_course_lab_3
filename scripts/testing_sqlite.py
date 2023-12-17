import sqlite3
from csv_to_database import db_table_name

def execute_command(connection, print_flag, sql_request):
    cursor = connection.cursor()
    data = cursor.execute(sql_request).fetchall()

    if print_flag:
        for item in data:
            print(item)

    connection.commit()


def q1_sqlite(connection, print_flag=False):
    sql_request = f"SELECT vendorid, COUNT(*) FROM {db_table_name} GROUP BY 1;"

    execute_command(connection, print_flag, sql_request)

def q2_sqlite(connection, print_flag=False):
    sql_request = f"SELECT passenger_count, avg(total_amount) FROM {db_table_name} GROUP BY 1;"

    execute_command(connection, print_flag, sql_request)


def q3_sqlite(connection, print_flag=False):
    sql_request = f"""SELECT passenger_count,
                        strftime('%Y', tpep_pickup_datetime),
                        COUNT(*)
                        FROM {db_table_name}
                        GROUP BY 1, 2;"""

    execute_command(connection, print_flag, sql_request)


def q4_sqlite(connection, print_flag=False):
    sql_request = f"""SELECT
                    passenger_count,
                    strftime('%Y', tpep_pickup_datetime),
                    round(trip_distance),
                    count(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2, 3
                    ORDER BY 2, 4 DESC"""

    execute_command(connection, print_flag, sql_request)


if __name__ == "__main__":
    conn = sqlite3.connect(f"../data/converted/{db_table_name}.db")

    q4_sqlite(conn, True)

    conn.close()