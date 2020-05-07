import psycopg2
import csv

# I had some functions up here but decided they wouldn't work out. Basically just use the built-in
# open() and call read() on the file object, etc. etc.

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
connection = psycopg2.connect(connection_string)


def import_schema():
    with connection.cursor() as cursor:
        schema = open('schema.sql', 'r').read()
        cursor.execute(schema)
        connection.commit()


def convert_wtype(wtype: str) -> str:
    if wtype != "":
        return 1
    return 0

def insert_str(table: str, *insertions) -> None:
    percent_vals = ", ".join(("%s" for i in range(len(insertions))))
    connection.cursor().execute(f"INSERT INTO {table} VALUES ({percent_vals})", insertions)


def import_weather():
    cur = connection.cursor()
    with open('datasets/weather.csv', 'r') as weather_file:
        file_reader = csv.reader(weather_file)
        next(file_reader)  # Skip the header row.
        for row in file_reader:
            # insert into Weather
            insert_str("Weather", row[0], row[1])
            insert_str("Wind", row[1], row[2] if row[2] != "" else 0)
            insert_str("Precipitation", row[1], row[4], row[5] if row[5] != "" else 0, row[6])
            insert_str("Temperature", row[1], row[8], row[9])
            # Wtypes
            wtypes = [row[1]]
            for wtype in row[11:]:
                wtypes.append(convert_wtype(wtype))
            cur.execute(
                "INSERT INTO Wtypes VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", wtypes
            )
    connection.commit()


def null_number(s):
    if s == "":
        return 1
    return 0

def import_crashes():
    cur = connection.cursor()
    with open('user_accounts.csv', 'r') as crash_file:
        file_reader = csv.reader(crash_file)
        next(file_reader)  # Skip the header row.
        for row in file_reader:
            # Check for missing latitude and longitude
            if null_number(row[4]):
                row[4] = 0

            if null_number(row[5]):
                row[5] = 0

            cur.execute(
                "INSERT INTO Crash VALUES(%s, %s, %s)", (row[23], row[0], row[1])
            )

            cur.execute(
                "INSERT INTO Location VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
                (row[23], row[2], row[3], row[4], row[5], row[7], row[8], row[9])
            )

            cur.execute(
                "INSERT INTO Injuries VALUES(%s, %s, %s, %s, %s)", (row[23], row[10], row[12], row[14], row[16])
            )

            cur.execute(
                "INSERT INTO Deaths VALUES(%s, %s, %s, %s, %s)", (row[23], row[11], row[13], row[15], row[17])
            )

            cur.execute(
                "INSERT INTO VehiclesFactors VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (row[23], row[24], row[25], row[26], row[27], row[28],
                row[18], row[19], row[20], row[21], row[22])
            )
    connection.commit()


def main():
    import_schema()
    import_weather()
    import_crashes()


if __name__ == "__main__":
    main()
