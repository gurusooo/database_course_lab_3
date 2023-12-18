from time import time
import psycopg2
import pandas as pd
import duckdb
import sqlalchemy
import sqlite3
from statistics import median
import os
import matplotlib.pyplot as plt
import numpy as np
from scripts.read_settings import get_postgres_data, get_test_list, get_test_settings, get_path_to_csv
from scripts.csv_to_database import to_postgres_and_sqlite
from scripts.testing_psycopg2 import q1_psycopg2, q2_psycopg2, q3_psycopg2, q4_psycopg2
from scripts.testing_sqlite import q1_sqlite, q2_sqlite, q3_sqlite, q4_sqlite
from scripts.testing_duckdb import q1_duckdb, q2_duckdb, q3_duckdb, q4_duckdb
from scripts.testing_pandas import q1_pandas, q2_pandas, q3_pandas, q4_pandas
from scripts.testing_sqlalchemy import q1_sqlalchemy, q2_sqlalchemy, q3_sqlalchemy, q4_sqlalchemy

# get path to csv file
path = get_path_to_csv()
absolute_path = os.path.abspath("../" + path)

# prepare name for table
db_table_name = path.lstrip("data/raw/")
db_table_name = db_table_name.rstrip(".csv")

postgres = get_postgres_data()
postgres_access = f"postgresql://{postgres['user=']}:{postgres['password=']}@" \
           f"{postgres['host=']}:{postgres['port=']}/{postgres['name=']}"
sqlite_access = f"../data/converted/{db_table_name}.db"




# Step 1: Copy data from csv to postgres and create sqlite database
to_postgres_and_sqlite()

# Step 2: Get test info and prepare variables to store time
tests = get_test_list()
test_settings = get_test_settings()
recorded_time = {"psycopg2=": [[], [], [], []],
                 "sqlite=": [[], [], [], []],
                 "duckdb=": [[], [], [], []],
                 "pandas=": [[], [], [], []],
                 "sqlalchemy=": [[], [], [], []]}

# Step 3: Start testing
list_to_test = get_test_list()
# Psycopg2
if list_to_test["psycopg2="]:
    print("\nPsycopg2 Testing started...")
    conn = psycopg2.connect(postgres_access)
    for _ in range(test_settings["test_qty="]):

        start = time()
        q1_psycopg2(conn, db_table_name, test_settings["print_results="])
        recorded_time["psycopg2="][0].append(time() - start)

        start = time()
        q2_psycopg2(conn, db_table_name, test_settings["print_results="])
        recorded_time["psycopg2="][1].append(time() - start)

        start = time()
        q3_psycopg2(conn, db_table_name, test_settings["print_results="])
        recorded_time["psycopg2="][2].append(time() - start)

        start = time()
        q4_psycopg2(conn, db_table_name, test_settings["print_results="])
        recorded_time["psycopg2="][3].append(time() - start)

    conn.close()
    print("Psycopg2 Testing finished\n")

if list_to_test["sqlite="]:
    # SQLite
    print("SQLite Testing started...")
    conn = sqlite3.connect(f"data/converted/{db_table_name}.db")
    for _ in range(test_settings["test_qty="]):

        start = time()
        q1_sqlite(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlite="][0].append(time() - start)

        start = time()
        q2_sqlite(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlite="][1].append(time() - start)

        start = time()
        q3_sqlite(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlite="][2].append(time() - start)

        start = time()
        q4_sqlite(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlite="][3].append(time() - start)

    conn.close()
    print("SQLite Testing finished\n")



# DuckDB
if list_to_test["duckdb="]:
    print("DuckDB Testing started...")
    duckdb.sql("INSTALL postgres")
    duckdb.sql(f"CALL postgres_attach('{postgres_access}')")
    for _ in range(test_settings["test_qty="]):

        start = time()
        q1_duckdb(db_table_name, test_settings["print_results="])
        recorded_time["duckdb="][0].append(time() - start)

        start = time()
        q2_duckdb(db_table_name, test_settings["print_results="])
        recorded_time["duckdb="][1].append(time() - start)

        start = time()
        q3_duckdb(db_table_name, test_settings["print_results="])
        recorded_time["duckdb="][2].append(time() - start)

        start = time()
        q4_duckdb(db_table_name, test_settings["print_results="])
        recorded_time["duckdb="][3].append(time() - start)
    print("DuckDB Testing finished\n")


# Pandas
if list_to_test["pandas="]:
    print("Generating file for Pandas")
    pd.options.mode.chained_assignment = None
    engine = sqlalchemy.create_engine(postgres_access)
    sql = f"SELECT * FROM {db_table_name}"
    df = pd.read_sql(sql, con=engine)
    print("Pandas Testing started...")
    for _ in range(test_settings["test_qty="]):

        start = time()
        q1_pandas(df, test_settings["print_results="])
        recorded_time["pandas="][0].append(time() - start)

        start = time()
        q2_pandas(df, test_settings["print_results="])
        recorded_time["pandas="][1].append(time() - start)

        start = time()
        q3_pandas(df, test_settings["print_results="])
        recorded_time["pandas="][2].append(time() - start)

        start = time()
        q4_pandas(df, test_settings["print_results="])
        recorded_time["pandas="][3].append(time() - start)

    engine.dispose()
    del df
    print("Pandas Testing finished\n")


# SQLAlchemy
if list_to_test["sqlalchemy="]:
    print("SQLAlchemy Testing started...")
    engine = sqlalchemy.create_engine(postgres_access)
    metadata = sqlalchemy.MetaData()
    metadata.create_all(engine)
    conn = engine.connect()

    for _ in range(test_settings["test_qty="]):

        start = time()
        q1_sqlalchemy(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlalchemy="][0].append(time() - start)

        start = time()
        q2_sqlalchemy(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlalchemy="][1].append(time() - start)

        start = time()
        q3_sqlalchemy(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlalchemy="][2].append(time() - start)

        start = time()
        q4_sqlalchemy(conn, db_table_name, test_settings["print_results="])
        recorded_time["sqlalchemy="][3].append(time() - start)

    conn.close()
    engine.dispose()
    print("SQLAlchemy Testing finished\n")


# Step 4: Output results
# Plot Bar Chart
queries = ("Q1", "Q2", "Q3", "Q4")
plot_data = {}
max_val = 0
for key in recorded_time:
    if list_to_test[key]:
        temp_data = [round(median(recorded_time[key][0]) * 1000, 0), round(median(recorded_time[key][1]) * 1000, 0),
                     round(median(recorded_time[key][2]) * 1000, 0), round(median(recorded_time[key][3]) * 1000, 0)]

        temp_max = max(temp_data)
        max_val = temp_max if temp_max > max_val else max_val
        plot_data.setdefault(key.strip("="), tuple(temp_data))

x = np.arange(len(queries))  # the label locations
width = 0.18  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout='constrained')

for library, measurement in plot_data.items():
    offset = width * multiplier
    rects = ax.bar(x + offset, measurement, width, label=library)
    ax.bar_label(rects, padding=3)
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Milliseconds (mm)')
ax.set_title('Benchmark Speedups for Python Database Libraries')
ax.set_xticks(x + width, queries)
ax.legend(loc='upper left', ncols=4)
ax.set_ylim(0, max_val * 1.3)

plt.savefig(f"output/img/{db_table_name}.png")
plt.close(fig)

with open(f"output/report_data_{db_table_name}.txt", "wt", encoding="utf-8") as file:
    file.write("Psycopg2:\n")
    for item in recorded_time["psycopg2="]:
        file.write(f"{item}\n")

    file.write("\nSQLite:\n")
    for item in recorded_time["sqlite="]:
        file.write(f"{item}\n")

    file.write("\nDuckDB:\n")
    for item in recorded_time["duckdb="]:
        file.write(f"{item}\n")

    file.write("\nPandas:\n")
    for item in recorded_time["pandas="]:
        file.write(f"{item}\n")

    file.write("\nSQLAlchemy:\n")
    for item in recorded_time["sqlalchemy="]:
        file.write(f"{item}\n")
