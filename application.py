from database import Database


def main():
    """
    Run the actual application and deal with user input.
    This should not be run until retrieve.py, database.py, and load_data.py (in that order)
    :return:null
    """

    app = Database()
    print("Application Loaded\nWelcome User")

    # NEW CODE
    # ------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------

    query_options = [
        ("Crashes by date", app.crashesByDate),
        ("Weather by date", app.weatherByDate),
        ("Most common weather", app.mostCommonWeather),
        ("Most crashed in weather", app.crashesByWeather),
        ("Deadliest weather", app.deadliestWeather)
    ]

    print("\nEnter the number of the query you would like to execute, or type quit:")
    current = 1
    for option in query_options:
        print("{}. {}".format(current, option[0]))
        current += 1


    while (True):
        user_query_selection = input("\nSelection: ")

        if (type(user_query_selection) != int or user_query_selection < 1 or user_query_selection > len(query_options)-1):
            print("Invalid selection.")
            continue

        query_options[user_query_selection][1]()


    # ------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------


    # ---Begin User Input--- #
    user_query_selection = -1
    while user_query_selection != 0:
        user_query_selection = input("\n1. Query 1"
                                     "\n2. Query 2"
                                     "\n3. Query 3"
                                     "\n4. Query 4"
                                     "\n5. Query 5"
                                     "\nPlease select an option, or enter 0 to exit:")

        # ---Error Checking for input--- #
        # Handle non integer input
        if type(user_query_selection) != int:
            user_query_selection = -1
            print("Error: Please enter a valid choice")
            continue

        # Handle an out of bounds input
        if user_query_selection > 5 or user_query_selection < 0:
            user_query_selection = -1
            print("Error: Please enter an integer within the range of selection [0-5]")
            continue

        # ---Translate Input Into Running The Query--- #
        if user_query_selection == 1:
            print("Run Query 1")
        elif user_query_selection == 2:
            print("Run Query 2")
        elif user_query_selection == 3:
            print("Run Query 3")
        elif user_query_selection == 4:
            print("Run Query 4")
        elif user_query_selection == 5:
            print("Run Query 5")


if __name__ == "__main__":
    main()
