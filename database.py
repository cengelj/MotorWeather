import psycopg2


typecodes = {
    "WT01": "Fog, ice fog, or freezing fog (may include heavy fog)",
    "WT02": "Heavy fog or heavy freezing fog (not always distinguished from fog)",
    "WT03": "Thunder",
    "WT04": "Ice pellets, sleet, snow pellets, or small hail",
    "WT06": "Glaze or rime",
    "WT08": "Smoke or haze",
    "WT11": "High or damaging winds",
    "WT13": "Mist",
    "WT14": "Drizzle",
    "WT16": "Rain (may include freezing rain, drizzle, and freezing drizzle)",
    "WT18": "Snow, snow pellets, snow grains, or ice crystals",
    "WT19": "Unknown source of precipitation",
    "WT22": "Ice fog or freezing fog"
}



class Database:
    """
    Used to connect to the database and run queries on the information within
    """
    _connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

    def __init__(self):
        """
        Constructor for the application
        """
        self._connection = psycopg2.connect(self._connection_string)

    
    def execute_query(self, query, *args):
        """
        Executes the given query with the given arguments on the database
        :param query: The query to be executed on the database, any user inputted data should come in the form of %s
        :param args: The user inputted data to use in place of %s
        :return: The results of the query
        """
        with self._connection.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.description, cursor.fetchall()


    def weatherByDate(self):
        date = input("Enter a date (YYYY/MM/DD) to gather the weather data:")

        # Gather data across tables
        data_query = """
        SELECT maxtemp, mintemp, precip, snow, snowdepth, avgwind
        FROM Temperature, Precipitation, Wind
        WHERE date = %s
        """

        # Gather weather type data
        weather_type_query = """
        SELECT * FROM Wtypes
        WHERE date = %s
        """

        # Various data across tables
        column_names, datapoints = self.execute_query(data_query, date)

        # Weather types table
        column_names, weather_counts = self.execute_query(weather_type_query, date)
        weather_types = [desc[0] for desc in column_names]
        type_with_count = list(zip(weather_types, weather_counts))


        print("High Temperature (F) {}", datapoints[0])
        print("Low Temperature  (F) {}", datapoints[1])
        print("Total precipitation  {}", datapoints[2])
        print("Total snow ......... {}", datapoints[3])
        print("Snow depth ......... {}", datapoints[4])
        print("Average Wind ....... {}", datapoints[5])

        print("\nWeather events:")

        for twc in type_with_count:
            # If the weather type occured on this day
            if (twc[1]):
                print("- {}".format(typecodes[twc[0]]))


    def mostCommonWeather(self):
        # Get the count of the chosen weather type
        query = """
        SELECT COUNT(%s) FROM Wtypes
        WHERE %s = 1
        """

        results = []

        # Add each type and its count to results
        for typecode in typecodes.keys():
            typecount = self.execute_query(query, (typecode, typecode))[0]
            results.append((typecode, typecount))

        results.sort(key=lambda t: t[1], reverse=True)  # Sort by count descending

        print("Most common weather conditions (descending):")

        # Print rank, description, and count for each weather type
        currentrank = 1
        for typecode in results:
            print("{}. {}: {} occurrences", currentrank, typecodes[typecode[0]], typecode[1])
            currentrank += 1

