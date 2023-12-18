# read postgres info to connect to database
def get_postgres_data():
    data = {"postgresql_enabled=": None, "name=": None, "host=": None, "port=": None, "user=": None, "password=": None}
    with open("settings.conf", "rt", encoding="utf-8") as file:
        while True:
            temp = file.readline().strip()
            if "# postgres" in temp:
                for i in range(6):
                    temp_data = file.readline().strip().split()
                    setting = temp_data[0].lower()
                    if setting == "postgresql_enabled=":
                        if temp_data[1] == "True":
                            data[setting] = True
                        else:
                            return None
                    else:
                        data[setting] = temp_data[1]
                return data


def get_path_to_csv():
    with open("settings.conf", "rt", encoding="utf-8") as file:

        while True:
            temp = file.readline().strip()
            if "# csv" in temp:
                temp_data = file.readline().strip().split()
                if len(temp_data) < 2:
                    return None
                else:
                    return temp_data[1]
            if temp is None:
                break


# read the info on tests (which libraries to test)
def get_test_list():
    # Check if CSV with data is available in settings file
    path_to_csv = get_path_to_csv()
    if path_to_csv is None:
        print(f"No path to .csv data file was mentioned in settings.conf!\nCannot proceed with testing!!!")
        return None

    data = {"psycopg2=": None, "sqlite=": None, "duckdb=": None, "pandas=": None, "sqlalchemy=": None}
    with open("settings.conf", "rt", encoding="utf-8") as file:
        while True:
            temp = file.readline().strip()
            if "# Choose libraries" in temp:
                for i in range(5):
                    temp_data = file.readline().strip().split()
                    setting = temp_data[0].lower()
                    data[setting] = True if temp_data[1] == "True" else False
                break

    # Check if all required files and databases available for testing
    postgres_enabled = get_postgres_data()
    if postgres_enabled is None:
        data["psycopg2="] = False
        data["duckdb="] = False
        data["pandas="] = False
        data["sqlalchemy="] = False
        print(f"PostgreSQL connection is not enabled! Therefore \"psycopg2\", \"DuckDB\", \"Pandas\" and \"SQLAlchemy\" libraries will not be tested")
    return data


def get_test_settings():
    data = {"test_qty=": None, "print_results=": False}
    with open("settings.conf", "rt", encoding="utf-8") as file:
        while True:
            temp = file.readline().strip()
            if "# Printing out" in temp:
                for i in range(2):
                    temp_data = file.readline().strip().split()
                    setting = temp_data[0].lower()
                    if setting == "print_results=":
                        data[setting] = True if temp_data[1] == "True" else False
                    else:
                        data[setting] = int(temp_data[1])
                return data
            if temp is None:
                return None
