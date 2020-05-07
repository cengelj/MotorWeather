from database import Database

app = Database()  # Object to interface with the database

# List of query options and their corresponding function objects
query_options = [
    ("Crashes by date", app.crashesByDate),
    ("Weather by date", app.weatherByDate),
    ("Most common weather", app.mostCommonWeather),
    ("Most crashed in weather", app.crashesByWeather),
    ("Deadliest weather", app.deadliestWeather)
]


def printQueryOptions(options):
    """
    Prints out numbered list of query options
    options is a list of tuples of the form ("option name", function object)
    :return:null
    """
    current = 1
    for option in options:
        print("{}. {}".format(current, option[0]))
        current += 1

    print("\nType quit to stop, or help to list the options again.")


def main():
    """
    Print options and execute user selections until quit is entered
    This should not be run until retrieve_data.py, and load_data.py have been run (in that order)
    :return:null
    """
    print("Enter the number of the query you would like to execute:")

    printQueryOptions(query_options)

    user_query_selection = ""

    while user_query_selection != "quit":
        user_query_selection = input("Selection: ")
        print()

        if user_query_selection == "help":
            printQueryOptions(query_options)
            continue

        if user_query_selection == "quit":
            continue
        
        try:
            user_query_selection = int(user_query_selection)
        except ValueError:
            print("Invalid selection")
            continue

        if user_query_selection < 1 or user_query_selection > len(query_options):
            print("Invalid selection.")
            continue

        # Execute selected query function
        query_options[user_query_selection - 1][1]()
        print()
        print()


if __name__ == "__main__":
    main()
