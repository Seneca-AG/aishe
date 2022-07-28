import psycopg2
from psycopg2 import Error

class Database:
    @staticmethod
    def connect_dbs():
        try:
            # Connect to an existing database
            connection = psycopg2.connect(user="post_user",
                                          password="post_user",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="aishtrading",
                                  options="-c search_path=aishtrading")

            # Create a cursor to perform database operations
            cursor = connection.cursor()

            # cursor.execute("DROP TABLE IF EXISTS words")
            # cursor.execute("CREATE TABLE words1(id SERIAL PRIMARY KEY, word VARCHAR(255))")
            # cursor.execute("INSERT INTO words1(word) VALUES('forest') RETURNING id")
            # cursor.execute("INSERT INTO words1(word) VALUES('cloud') RETURNING id")
            # cursor.execute("INSERT INTO words1(word) VALUES('valley') RETURNING id")
            #
            # last_row_id = cursor.fetchone()[0]
            #
            # print(f"The last Id of the inserted row is {last_row_id}")

            # Print PostgreSQL details
            print("PostgreSQL server information")
            print(connection.get_dsn_parameters(), "\n")
            # Executing a SQL query
            cursor.execute("SELECT * FROM audjpy_friday")
            record = cursor.fetchone()
            print("You are connected to - ", record, "\n")
            # list_tables = cursor.fetchall()
            #
            # for t_name_table in list_tables:
            #     print(t_name_table + "\n")
            # record = cursor.fetchone()
            # print("You are connected to - ", record, "\n")
            # SQL query to create a new table
            # create_table_query = '''CREATE TABLE aishtrading.mobile
            #           (ID INT PRIMARY KEY     NOT NULL,
            #           MODEL           TEXT    NOT NULL,
            #           PRICE         REAL); '''
            # # Execute a command: this creates a new table
            # cursor.execute(create_table_query)
            connection.commit()
            print("Table created successfully in PostgreSQL ")

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
                if (connection):
                    cursor.close()
                    connection.close()
                    print("PostgreSQL connection is closed")