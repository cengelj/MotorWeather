import psycopg2

typecodes = {
    "wt01": "Fog, ice fog, or freezing fog (may include heavy fog)",
    "wt02": "Heavy fog or heavy freezing fog (not always distinguished from fog)",
    "wt03": "Thunder",
    "wt04": "Ice pellets, sleet, snow pellets, or small hail",
    "wt06": "Glaze or rime",
    "wt08": "Smoke or haze",
    "wt11": "High or damaging winds",
    "wt13": "Mist",
    "wt14": "Drizzle",
    "wt16": "Rain (may include freezing rain, drizzle, and freezing drizzle)",
    "wt18": "Snow, snow pellets, snow grains, or ice crystals",
    "wt19": "Unknown source of precipitation",
    "wt22": "Ice fog or freezing fog"
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


    def format_weather_type_result(self, result):
        formatted_result = []
        for weather_index in range(len(result[1][0])):
            formatted_result.append([result[0][weather_index][0], result[1][0][weather_index]])

        formatted_result.sort(key=lambda t: t[1], reverse=True)
        return formatted_result


    def print_formatted_weather_ranking(self, result, describing_noun):
        current_rank = 1
        for weather_type in result:
            print("{} {}: {} {}".format("{}.".format(current_rank) if current_rank >= 10 else "{}. ".format(current_rank),
                                         typecodes[weather_type[0].lower()], weather_type[1], describing_noun))
            current_rank += 1 # Python depreciating ++ is a disgrace - Your friendly neighborhood python hater


    def weather_by_date(self):
        date = input("Enter a date (YYYY/MM/DD) to gather the weather data: ")

        # Gather data across tables
        data_query = """
        SELECT maxtemp, mintemp, precip, snow, snowdepth, avgwind
        FROM Temperature, Precipitation, Wind
        WHERE Temperature.date = %s
        AND Precipitation.date=Temperature.date
        AND Temperature.date=Wind.date
        """

        # Gather weather type data
        weather_type_query = """
        SELECT * 
        FROM Wtypes
        WHERE date = %s
        """

        # Various data across tables
        column_names, datapoints = self.execute_query(data_query, date)
        if len(datapoints)==0:
            print("Date has no weather data")
            return
        datapoints=datapoints[0]

        # Weather types table
        column_names, weather_counts = self.execute_query(weather_type_query, date)
        weather_types = [desc[0] for desc in column_names]
        type_with_count = list(zip(weather_types, weather_counts))

        print("High Temperature ......  {}°F".format(datapoints[0]))
        print("Low Temperature .......  {}°F".format(datapoints[1]))
        print("Total precipitation ...  {}".format(datapoints[2]))
        print("Total snow ............  {}".format(datapoints[3]))
        print("Snow depth ............  {}".format(datapoints[4]))
        print("Average Wind ..........  {}".format(datapoints[5]))

        print("\nWeather events:")
        for twc in type_with_count[2:]:
            # If the weather type occured on this day
            if twc[1]:
                print("- {}".format(typecodes[twc[0]]))

    def most_common_weather(self):
        # Get the count of the chosen weather type
        query = """
        SELECT SUM(w1.WT01) AS WT01, SUM(w1.WT02) AS WT02, SUM(w1.WT03) AS WT03, SUM(w1.WT04) AS WT04,
                SUM(w1.WT06) AS WT06, SUM(w1.WT08) AS WT08, SUM(w1.WT11) AS WT11, SUM(w1.WT13) AS WT13,
                SUM(w1.WT14) AS WT14, SUM(w1.WT16) AS WT16, SUM(w1.WT18) AS WT18, SUM(w1.WT19) AS WT19,
                SUM(w1.WT22) AS WT22 
        FROM Wtypes w1;
        """

        # Get result of query
        result = self.execute_query(query, ())

        # Format result in a form that can be easily sorted
        formatted_result = self.format_weather_type_result(result)

        print("Most common weather conditions (descending):")
        self.print_formatted_weather_ranking(formatted_result, "occurrence(s)")

    def crashes_by_date(self):
        print("Selected number of crashes on inputted date")
        input_date = input("Please enter a date (YYYY/MM/DD): ")
        result = self.execute_query("SELECT COUNT(id) FROM Crash WHERE \"date\" = %s", input_date)
        crash_total = result[1][0][0]
        print("There were " + str(crash_total) + " crashes on the date of " + str(input_date) + ".\n")


    def crashes_by_weather(self):
        print("Selected most crashed in weather conditions")
        query = """
        SELECT SUM(w1.WT01) AS WT01, SUM(w1.WT02) AS WT02, SUM(w1.WT03) AS WT03, SUM(w1.WT04) AS WT04,
                SUM(w1.WT06) AS WT06, SUM(w1.WT08) AS WT08, SUM(w1.WT11) AS WT11, SUM(w1.WT13) AS WT13, 
                SUM(w1.WT14) AS WT14, SUM(w1.WT16) AS WT16, SUM(w1.WT18) AS WT18, SUM(w1.WT19) AS WT19, 
                SUM(w1.WT22) AS WT22
        FROM (SELECT cD.count*w0.WT01 AS WT01, cD.count*w0.WT02 AS WT02, cD.count*w0.WT03 AS WT03,
                     cD.count*w0.WT04 AS WT04, cD.count*w0.WT06 AS WT06, cD.count*w0.WT08 AS WT08, 
                     cD.count*w0.WT11 AS WT11, cD.count*w0.WT13 AS WT13, cD.count*w0.WT14 AS WT14, 
                     cD.count*w0.WT16 AS WT16, cD.count*w0.WT18 AS WT18, cD.count*w0.WT19 AS WT19, 
                     cD.count*w0.WT22 AS WT22  
                FROM (SELECT c0.date, count(c0.date)
		        FROM Crash c0
		        GROUP BY c0.date) AS cD,
            Wtypes w0
	    WHERE w0.date=cD.date) AS w1;
        """
        result = self.execute_query(query, ())
        formatted_result = self.format_weather_type_result(result)

        # Print results (descending)
        print("Most Crashed In Weather Conditions(Descending):")
        self.print_formatted_weather_ranking(formatted_result, "crash(es)")

    def select_group(self, flag):
        print("{} for which group?\n".format(flag))
        groups = ["Total", "Pedestrians", "Cyclists", "Motorists"]
        current_number = 1
        for group in groups:
            print("{}. {}".format(current_number, groups[current_number - 1]))
            current_number += 1
        try:
            group_selected_identifier = int(input("\nSelection: "))
        except ValueError:
            print("Error Invalid Selection")
            raise ValueError
        return groups[group_selected_identifier - 1].lower()

    def deadliest_weather(self):
        print("Selected deadliest weather conditions")
        try:
            group_selection = self.select_group("Deadliest")
        except ValueError:
            return

        query = """
        SELECT SUM(s0.WT01) AS WT01, SUM(s0.WT02) AS WT02, SUM(s0.WT03) AS WT03, SUM(s0.WT04) AS WT04,
                SUM(s0.WT06) AS WT06, SUM(s0.WT08) AS WT08, SUM(s0.WT11) AS WT11, SUM(s0.WT13) AS WT13, 
                SUM(s0.WT14) AS WT14, SUM(s0.WT16) AS WT16, SUM(s0.WT18) AS WT18, SUM(s0.WT19) AS WT19, 
                SUM(s0.WT22) AS WT22
        FROM (SELECT deadly_crashes.date, WT01*deadly_crashes.sum AS WT01, WT02*deadly_crashes.sum AS WT02, 
                WT03*deadly_crashes.sum AS WT03, WT04*deadly_crashes.sum AS WT04, WT06*deadly_crashes.sum AS WT06, 
                WT08*deadly_crashes.sum AS WT08, WT11*deadly_crashes.sum AS WT11, WT13*deadly_crashes.sum AS WT13, 
                WT14*deadly_crashes.sum AS WT14, WT16*deadly_crashes.sum AS WT16, WT18*deadly_crashes.sum AS WT18, 
                WT19*deadly_crashes.sum AS WT19, WT22*deadly_crashes.sum AS WT22
	        FROM (SELECT c0.date, SUM(d0.{})
		    FROM Crash c0, Deaths d0
		    WHERE c0.id=d0.id
		    GROUP BY c0.date) AS deadly_crashes, 
		    Wtypes t0
	    WHERE t0.date=deadly_crashes.date) AS s0;
        """.format(group_selection)

        result = self.execute_query(query, ())
        formatted_result = self.format_weather_type_result(result)

        # Print results (descending)
        print("Deadliest Weather Conditions ({}, Descending):".format(group_selection))
        self.print_formatted_weather_ranking(formatted_result, "death(s)")

    def most_injuries_weather(self):
        print("Selected most injured in weather conditions")
        try:
            group_selection = self.select_group("Most injuries")
        except ValueError:
            return

        query = """
        SELECT SUM(s0.WT01) AS WT01, SUM(s0.WT02) AS WT02, SUM(s0.WT03) AS WT03, SUM(s0.WT04) AS WT04,
                SUM(s0.WT06) AS WT06, SUM(s0.WT08) AS WT08, SUM(s0.WT11) AS WT11, SUM(s0.WT13) AS WT13,
                SUM(s0.WT14) AS WT14, SUM(s0.WT16) AS WT16, SUM(s0.WT18) AS WT18, SUM(s0.WT19) AS WT19,
                SUM(s0.WT22) AS WT22
                FROM (SELECT injury_crashes.date, WT01*injury_crashes.sum AS WT01, WT02*injury_crashes.sum AS WT02,
                        WT03*injury_crashes.sum AS WT03, WT04*injury_crashes.sum AS WT04, WT06*injury_crashes.sum AS WT06,
                        WT08*injury_crashes.sum AS WT08, WT11*injury_crashes.sum AS WT11, WT13*injury_crashes.sum AS WT13,
                        WT14*injury_crashes.sum AS WT14, WT16*injury_crashes.sum AS WT16, WT18*injury_crashes.sum AS WT18,
                        WT19*injury_crashes.sum AS WT19, WT22*injury_crashes.sum AS WT22
	                    FROM (SELECT c0.date, SUM(i0.{})
		                        FROM Crash c0, Injuries i0
		                        WHERE c0.id=i0.id
		                        GROUP BY c0.date) AS injury_crashes, 
		                Wtypes t0
	            WHERE t0.date=injury_crashes.date) AS s0;
        """.format(group_selection)
        result = self.execute_query(query, ())
        formatted_result = self.format_weather_type_result(result)

        # Print results (descending)
        print("Most injured in Weather Conditions ({}, Descending):".format(group_selection))
        self.print_formatted_weather_ranking(formatted_result, "injury(s)")

