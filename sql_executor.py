# sql executor => script for execution of .sql scripts (multi query only!)

import pyodbc

def establish_connection(db_name,
                         server_name):
    driver = "{ODBC Driver 17 for SQL Server}"
    connection_string = f"Driver={driver};Server={server_name};Database={db_name};Trusted_Connection=yes;"
    return pyodbc.connect(connection_string)

def execute_sql_script(cursor,
                       sql_file,
                       separator):
    with open(sql_file, "r") as file:
        sql_script = file.read()

    sql_commands = sql_script.split(separator)
    for command in sql_commands:
        try:
            cursor.execute(command)
            cursor.commit()
        except pyodbc.Error as e:
            print(f"Error executing SQL command: {e}")

def sql_executor(db_name,
                 sql_file,
                 separator,
                 server_name="xxx"):
    try:
        connection = establish_connection(db_name,
                                          server_name)
        cursor = connection.cursor()

        execute_sql_script(cursor,
                           sql_file,
                           separator)
    finally:
        if 'connection' in locals():
            connection.close()

# specify database name, .sql script name and separator (sql code must have ";" or "go" at the end of the block of the code)
# example for sql section: delete from xxx where y = 1; or delete from xxx where y = 1;go
sql_executor('dbname',
             'assistance_duration.sql',
             'go')