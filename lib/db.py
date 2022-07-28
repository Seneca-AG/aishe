#!/usr/bin/env python3
import sys, psycopg2, psycopg2.sql as sql
from dotenv import load_dotenv
import os


class Crud:
    # Initialize the Global variables
    def __init__(self):
        load_dotenv()
        self.user = os.getenv('DBUSER')
        self.password = os.getenv('DBSPASSWORD')
        self.host = os.getenv('DBHOST')
        self.port = os.getenv('DBPORT')
        self.dbname = os.getenv('DATABASE')
        self.table = ''

    # Establish database connection
    def connect(self):
        try:
            connection = psycopg2.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.dbname,
                options="-c search_path="+self.dbname
            )
            cursor = connection.cursor()
            print(
                '------------------------------------------------------------'
                '\n-# PostgreSQL connection & transaction is ACTIVE\n'
            )
        except (Exception, psycopg2.Error) as error:
            print(error, error.pgcode, error.pgerror, sep='\n')
            sys.exit()
        else:
            self._connection = connection
            self._cursor = cursor
            self._counter = 0

    # Validate the database connection
    def _check_connection(self):
        try:
            self._connection
        except AttributeError:
            print('ERROR: NOT Connected to Database')
            sys.exit()

    # Set the table
    def settable(self, tablename):
        self.table = tablename

    # Execute the query
    def _execute(self, query, Placeholder_value=None):
        self._check_connection()
        if Placeholder_value == None or None in Placeholder_value:
            self._cursor.execute(query)
            print('-# ' + query.as_string(self._connection) + ';\n')
        else:
            self._cursor.execute(query, Placeholder_value)
            print('-# ' + query.as_string(self._connection) % Placeholder_value + ';\n')

    # Commit the SQL changes to database
    def commit(self):
        self._check_connection()
        self._connection.commit()
        print('-# COMMIT ' + str(self._counter) + ' changes\n')
        self._counter = 0

    # Close the database connection
    def close(self, commit=False):
        self._check_connection()
        if commit:
            self.commit()
        else:
            self._cursor.close()
            self._connection.close()
        if self._counter > 0:
            print(
                '-# ' + str(self._counter) + ' changes NOT commited  CLOSE connection\n'
                                             '------------------------------------------------------------\n'
            )
        else:
            print(
                '-# CLOSE connection\n'
                '------------------------------------------------------------\n'
            )

    # Insert records into a table
    def insert(self, **column_value):
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(self.table),
            sql.SQL(', ').join(map(sql.Identifier, column_value.keys())),
            sql.SQL(', ').join(sql.Placeholder() * len(column_value.values()))
        )
        record_to_insert = tuple(column_value.values())
        self._execute(insert_query, record_to_insert)
        self._counter += 1
        print(self._counter)

    # Bulk insert into a table
    def insert_many(self, columns, rows):
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(self.table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(rows[0]))
        )
        for row in rows:
            row = tuple(row)
            self._execute(insert_query, row)
            self._counter += 1

    # Fetch the records from the tables
    def select(self, columns, primaryKey_value=None):
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT {} FROM {}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.table)
            )
            self._execute(select_query)
        else:
            select_query = sql.SQL("SELECT {} FROM {} WHERE {} = {}").format(
                sql.SQL(',').join(map(sql.Identifier, columns)),
                sql.Identifier(self.table),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute(select_query, (primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print('-# ' + str(selected) + '\n')
            return selected

    # Fetch all the records from the tables
    def select_all(self, primaryKey_value=None):
        if primaryKey_value == None:
            select_query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(self.table))
            self._execute(select_query)
        else:
            select_query = sql.SQL("SELECT * FROM {} WHERE {} = {}").format(
                sql.Identifier(self.table),
                sql.Identifier(self.primarykey),
                sql.Placeholder()
            )
            self._execute(select_query, (primaryKey_value,))
        try:
            selected = self._cursor.fetchall()
        except psycopg2.ProgrammingError as error:
            selected = '# ERROR: ' + str(error)
        else:
            print('-# ' + str(selected) + '\n')
            return selected

    # Update the records from the tables
    def update(self, column, column_value, primaryKey_value):
        update_query = sql.SQL("UPDATE {} SET {} = {} WHERE {} = {}").format(
            sql.Identifier(self.table),
            sql.Identifier(column),
            sql.Placeholder(),
            sql.Identifier(self.primarykey),
            sql.Placeholder()
        )
        self._execute(update_query, (column_value, primaryKey_value))
        self._counter += 1

    # Update multiple columns in a tables
    def update_multiple_columns(self, columns, columns_value, primaryKey_value):
        update_query = sql.SQL("UPDATE {} SET ({}) = ({}) WHERE {} = {}").format(
            sql.Identifier(self.table),
            sql.SQL(',').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns_value)),
            sql.Identifier(self.primarykey),
            sql.Placeholder()
        )
        Placeholder_value = list(columns_value)
        Placeholder_value.append(primaryKey_value)
        Placeholder_value = tuple(Placeholder_value)
        self._execute(update_query, Placeholder_value)
        self._counter += 1

    # Delete the seleted record from the table
    def delete(self, primaryKey_value):
        delete_query = sql.SQL("DELETE FROM {} WHERE {} = {}").format(
            sql.Identifier(self.table),
            sql.Identifier(self.primarykey),
            sql.Placeholder()
        )
        self._execute(delete_query, (primaryKey_value,))
        self._counter += 1

    # Delete all records
    def delete_all(self):
        delete_query = sql.SQL("DELETE FROM {}").format(sql.Identifier(self.table))
        self._execute(delete_query)
        self._counter += 1
