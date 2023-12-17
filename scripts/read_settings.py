def get_postgres_data():
    data = {"postgresql_enabled=": None, "name=": None, "host=": None, "port=": None, "user=": None, "password=": None}
    with open("../settings.conf", "rt", encoding="utf-8") as file:
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
                            data[setting] = False
                            return None
                    else:
                        data[setting] = temp_data[1]
                return data

if __name__ == "__main__":
    print(get_postgres_data())



