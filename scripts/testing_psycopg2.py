def execute_command(connection, print_flag, sql_request):
    with connection.cursor() as cursor:
        cursor.execute(sql_request)

        if print_flag:
            for item in cursor.fetchall():
                print(item)

    connection.commit()


def q1_psycopg2(connection, db_table_name, print_flag=False):
    sql_request = f"SELECT \"VendorID\", COUNT(*) FROM {db_table_name} GROUP BY 1;"
    execute_command(connection, print_flag, sql_request)


def q2_psycopg2(connection, db_table_name, print_flag=False):
    sql_request = f"SELECT passenger_count, avg(total_amount) FROM {db_table_name} GROUP BY 1;"
    execute_command(connection, print_flag, sql_request)


def q3_psycopg2(connection, db_table_name, print_flag=False):
    sql_request = f"""SELECT passenger_count,
                    extract(year from tpep_pickup_datetime),
                    COUNT(*)
                    FROM {db_table_name}
                    GROUP BY 1, 2;"""

    execute_command(connection, print_flag, sql_request)


def q4_psycopg2(connection, db_table_name, print_flag=False):
    sql_request = f"""SELECT
                passenger_count,
                extract(year from tpep_pickup_datetime),
                round(trip_distance),
                count(*)
                FROM {db_table_name}
                GROUP BY 1, 2, 3
                ORDER BY 2, 4 DESC"""

    execute_command(connection, print_flag, sql_request)
