import pandas as pd
import psycopg2
import os
import sqlite3
import csv
from scripts.read_settings import get_path_to_csv, get_postgres_data


# get postgres connection info
postgres = get_postgres_data()

# get path to csv file
path = get_path_to_csv()
absolute_path = os.path.abspath("../" + path)

# prepare name for table
db_table_name = path.lstrip("data/raw/")
db_table_name = db_table_name.rstrip(".csv")

def to_postgres_and_sqlite():
    # read from csv
    data = pd.read_csv(r"" + get_path_to_csv(), nrows=2)

    # prepare headers names
    headers = list(data.columns)

    # rename replicas
    for i in range(len(headers)-1, -1, -1):
        temp = headers[i].lower()
        if temp in headers[0:i]:
            headers[i] += "2"

    # data types conversion from pandas to postgres
    data_types = {"int64": "int", "float64": "float"}

    # prepare types of columns
    column_types = []
    for i, item in enumerate(data.dtypes):
        if "object" in item.name:
            if "time" in headers[i]:
                column_types.append("timestamp")
            elif "flag" in headers[i]:
                column_types.append("varchar(1)")
        else:
            column_types.append(data_types[item.name])

    # generate sql request to delete and create table
    sql_request_delete_table = f"DROP TABLE IF EXISTS {db_table_name};"
    sql_request_create_table = f"CREATE TABLE {db_table_name}("
    sql_request_copy_data = f"COPY {db_table_name} FROM STDIN DELIMITER ',' CSV HEADER;"
    sqlite_request_insert_records = f"INSERT INTO {db_table_name} VALUES ("

    for i in range(len(headers)):
        sql_request_create_table += f"\"{headers[i]}\" {column_types[i]}"
        if i != len(headers) - 1:
            sql_request_create_table += ", "
            sqlite_request_insert_records += "?, "

        else:
            sql_request_create_table += ");"
            sqlite_request_insert_records += "?)"

    # connect to postgres database
    conn = psycopg2.connect(database=postgres["name="],
                            user=postgres["user="],
                            password=postgres["password="],
                            host=postgres["host="],
                            port=postgres["port="])
    conn.autocommit = True

    print(f"Copying to PostgreSQL started ...")
    # print(sql_request_copy_data)
    with conn.cursor() as cursor:
        cursor.execute(sql_request_delete_table)
        cursor.execute(sql_request_create_table)
        # cursor.execute(sql_request_copy_data)
        with open(f"" + path, "r") as csv_file:
            cursor.copy_expert(sql=sql_request_copy_data, file=csv_file)

    print(f"Copying to PostgreSQL completed!")

    conn.commit()
    conn.close()


    # creating or opening sqlite database
    sqlite_db_path = f"data/converted/{db_table_name}.db"
    file = open(path, "r")
    contents = csv.reader(file)
    next(contents)

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Fill the database
    print(f"Copying to SQLite {db_table_name}.db database started ...")
    cursor.execute(sql_request_delete_table)
    cursor.execute(sql_request_create_table)
    cursor.executemany(sqlite_request_insert_records, contents)
    print(f"Copying to SQLite {db_table_name}.db database completed!")

    file.close()
    conn.commit()
    conn.close()
