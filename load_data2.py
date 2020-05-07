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


def import_weather():
    cur = connection.cursor()
    with open('datasets/weather.csv', 'r') as weather_file:
        file_reader = csv.reader(weather_file)
        next(file_reader)  # Skip the header row.
        for row in file_reader:
            # insert into Weather
            print(row)
            cur.execute(
                "INSERT INTO Weather VALUES (%s, %s)", (row[0], row[1])
            )
            # wind
            cur.execute(
                "INSERT INTO Wind VALUES (%s, %s)", (row[1], row[2])
            )
            # precipitation
            cur.execute(
                "INSERT INTO Precipitation VALUES (%s, %s, %s, %s)", (row[1], row[4], row[5], row[6])
            )
            # Temperature
            cur.execute(
                "INSERT INTO Temperature VALUES (%s, %s, %s)", (row[1], row[8], row[9])
            )
            # Wtypes
            wtypes = [row[1]]
            for wtype in row[11:]:
                wtypes.append(convert_wtype(wtype))
            cur.execute(
                "INSERT INTO Wtypes VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", wtypes
            )
    connection.commit()


def import_crashes():
    cur = connection.cursor()
    with open('user_accounts.csv', 'r') as crash_file:
        file_reader = csv.reader(crash_file)
        next(file_reader)  # Skip the header row.
        for row in file_reader:
            # crash
            # location
            # injuries
            # Injuries
            # Deaths
            # VehicleFactors
            cur.execute(
                "INSERT INTO users VALUES (%s, %s, %s, %s)",
                row
            )
    connection.commit()


def main():
    import_schema()
    import_weather()


if __name__ == "__main__":
    main()
