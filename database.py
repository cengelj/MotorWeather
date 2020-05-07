import psycopg2


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
