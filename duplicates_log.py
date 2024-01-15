#### main script for transforming duplicates in SQL:

# import necessary modules and libraries:
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd, datetime as dt

# main sql function for connection:
def sql_conn():
    driver = "{ODBC Driver 17 for SQL Server}"
    server_name = "xxx"
    db_name = "yyy"
    # creation of the engine:
    sql_driver = (driver.replace("{", "")
                 .replace("}", ""))
    sql_path = f'mssql://@{server_name}/{db_name}?driver={sql_driver}'
    engine = (
        create_engine(
            sql_path))
    return engine

# function to fetch data from a sql table:
def get_data():
    Session = (
        sessionmaker(
            bind = sql_conn()))
    with Session() as session:
        query = 'SELECT * FROM duplicates_log_main'
        df = (pd.read_sql(query,
                         con = sql_conn()))
    return df

# function to replace [] within table:
def replace(column = 'cols'):
    df = get_data()
    df[column] = (df[column].
                  apply(lambda x: x.replace("[", "").
                        replace("]", "")))
    return df

# function to extract index:
def index_value(obj, val):
    try: return obj.split(",").index(val)
    except: return 0

# function to apply index_value function:
def apply_index_value():
    df = replace()
    df['received_at_count'] = (df['cols']
                               .apply(lambda x:
                                      index_value(x, 'received_at')))
    df['submission_id_count'] = (df['cols'].
                                 apply(lambda x: 
                                       index_value(x, 'submission_id')))
    return df

# function to affect created columns:
def value(x, p1, p2):
    cn, cv = x[p1], x[p2].split(",")
    return cv[cn]

# function to apply value function:
def apply_value():
    df = apply_index_value()
    df['received_at'] = (df.
                         apply(value,
                               axis = 1,
                               args = (5, 3)))
    df['submission_id'] = (df.
                           apply(value,
                                 axis = 1,
                                 args = (6, 3)))
    return df

# function to apply lenght logic:
def len_logic():
    df = apply_value()
    df['received_at'] = (df['received_at']
                         .apply(lambda x: 
                                x if len(x) == 26 else 'N/A'))
    df['submission_id'] = (df['submission_id']
                           .apply(lambda x: 
                                  x if len(x) == 7 else 'N/A'))
    return df

# function for final prep of the df:
def wrap_up():
    ls = ['db_name',
          'tbl_name',
          'received_at',
          'submission_id',
          'duplicates_count']
    df = len_logic()
    df.drop(['cols',
             'col_values',
             'received_at_count',
             'submission_id_count'],
            axis = 1,
            inplace = True)
    df = df.reindex(columns = ls)
    return df

# function to load data within sql table:
def load_data():
    dataframe = wrap_up()
    Session = (
        sessionmaker(
            bind = sql_conn()))
    with Session() as session:
        dataframe.to_sql('DUPLICATES_RESULT', 
                 sql_conn(), 
                 if_exists = 'replace',
                 index = False,
                 chunksize = 100)
        print("Data imported within SQL!")

# function to log the executions:
def write_file(filename):
    with open(filename, 'a') as f:          
        f.write('\n' + str(dt.datetime.now()) + ': script executed')

# calling final functions:
load_data()
write_file('log.txt')