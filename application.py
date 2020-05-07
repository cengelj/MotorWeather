from database import Database


printQueryOptions(options):
    print()

    current = 1
    for option in options:
        print("{}. {}".format(current, option[0]))
        current += 1

    print()


def main():
    """
    Run the actual application and deal with user input.
    This should not be run until retrieve.py, database.py, and load_data.py (in that order)
    :return:null
    """

    app = Database()
    print("Application Loaded\nWelcome User")
    

    query_options = [
        ("Crashes by date", app.crashesByDate),
        ("Weather by date", app.weatherByDate),
        ("Most common weather", app.mostCommonWeather),
        ("Most crashed in weather", app.crashesByWeather),
        ("Deadliest weather", app.deadliestWeather)
    ]

    print("\nEnter the number of the query you would like to execute:")

    printQueryOptions(query_options)

    print("\nType quit to stop, or help to list the options again.\n")

    user_query_selection = ""

    while (user_query_selection != "quit"):
        user_query_selection = input("\nSelection: ")

        if (user_query_selection == "help"):
            printQueryOptions(query_options)
            continue

        if (user_query_selection == "quit"):
            continue

        if (type(user_query_selection) != int or user_query_selection < 1 or user_query_selection > len(query_options)-1):
            print("Invalid selection.")
            continue

        query_options[user_query_selection][1]()




if __name__ == "__main__":
    main()
