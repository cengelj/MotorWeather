import psycopg2


class Application:
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
        with self.connection.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall()


def main():
    """
    Run the actual application and deal with user input.
    This should not be run until retrieve.py, database.py, and load_data.py (in that order)
    :return:null
    """
    app = Application()
    print("Application Loaded\nWelcome User")
    # ---Begin User Input--- #
    user_query_selection = -1
    while user_query_selection != 0:
        user_query_selection = input("\n1. Query 1"
                                     "\n2. Query 2"
                                     "\n3. Query 3"
                                     "\n4. Query 4"
                                     "\n5. Query 5"
                                     "Please select an option, or enter 0 to exit:")
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
        if user_query_selection == 2:
            print("Run Query 2")
        if user_query_selection == 3:
            print("Run Query 3")
        if user_query_selection == 4:
            print("Run Query 4")
        if user_query_selection == 5:
            print("Run Query 5")


if __name__ == "__main__":
    main()
