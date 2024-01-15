#### SQL table creator script for new and empty DM forms without submmissions ####

#### IMPORTANT NOTES:
# 1. Following non standard modules/libraries must be installed [requests and flatten_json]
# 2. DM forms needs to have at least 1 submission
# 3. It is highly recommended to use this script only for new forms
#    and first test submission needs to have questions populated with all answers
#    in order to catch all columns for future SQL table and script is fetching first submission by default
# 4. There are only two functions which requires parameters:
#    a) Function main_request(form_id, api_token) has two parameters:
#       first parameter is needed for form_id
#       second parameter is intented for api_token key specific for organization/account itself
#       this function is initiated through check_submissions() function
#    b) Function create_tbl_statement(db_name, tbl_name, approach) has tree parameters:
#       - first parameter requres a precise db_name or database name (database name needs to be the existing one in SQL server)
#       - second parameter needs any custom SQL tbl_name (use short as possible)
#       - and third parameter is an approach which can be either 1 or 2
#       approach == 1 will create sql table with nvarchar(MAX) dtypes for all columns
#       approach == 2 will create sql table with specified dtypes from DM form schema
#       this function is initiated within save_file() function


# main modules/libraries to use:
import subprocess as sp, sys, requests, re, time
from flatten_json import flatten

# function to extract DM form schema using API:
def main_request(form_id, api_token):
    url = "url/" + \
          form_id + \
          "/url"
    headers = {"Authorization": api_token}
    response = requests.get(url + "?page = 0", headers = headers)
    return flatten(response.json())

# function to check submissions and to call previous function main_request():
def check_submissions():

    ################################################
    # Parameters => DM form id and API token number!
    data = main_request("xxx",
                        "yyy")
    # Parameters => DM form id and API token number!
    ################################################

    if data["total_count"] == 0:
        print("Form needs to have at least 1 submission, please submit one entry!")
        sys.exit()
    else: return data

# function to separate and clean initial data:
def initial_prep():
    sub_list_keys, sub_list_values = [], []
    for x, y in check_submissions().items():
        if x.startswith("submissions_0_") and \
        x.endswith("_type") and \
        y != "subform":
            sub_list_keys.append(x)
            sub_list_values.append(y)
    return sub_list_keys, sub_list_values

# function to check and to prepare full data:
def prep_and_check_full_data():
    sub_list_keys, sub_list_values = initial_prep()
    sub_list_final_keys = []
    for x in sub_list_keys:
        x = re.sub("submissions_0_metadata_", '', x)
        x = re.sub("submissions_0_submission_", '', x)
        x = re.sub("_type", '', x)
        sub_list_final_keys.append(x)
    maximum = max([len(x) for x in sub_list_final_keys])
    if maximum >= 128:
        print("Question id is too long, SQL column name can take up to 128 characters!")
    else:
        pass
    try:
        idx = sub_list_final_keys.index("username")
        sub_list_final_keys.pop(idx)
        sub_list_values.pop(idx)
    except:
        pass
    if len(sub_list_final_keys) == len(sub_list_values):
        return sub_list_final_keys, sub_list_values
    else:
        print("Non equal number of question ids and data types, check the form!")
        sys.exit()

# function to further clean and prepare keys:
def cleaning_keys():
    sub_list_final_keys = prep_and_check_full_data()[0]
    finale = []
    for y in sub_list_final_keys:
        if "_0_" in y:
            y = y.split("_0_", 1)[1]
        else:
            y = y
        finale.append(y)
    final_ids = []
    for z in finale:
        if "_0_" in z:
            z = z.split("_0_", 1)[1]
        else:
            z = z
        final_ids.append(z)
    try:
        final_identifiers = []
        for x in finale:
            if "_0_" in x:
                x = x.split("_0_", 1)[1]
            else:
                x = x
            final_identifiers.append(x)
        return final_identifiers
    except: return final_identifiers

# function to prepare data types to be aligned with SQL dtypes:
def dtypes_prep():
    sub_list_values = prep_and_check_full_data()[1]
    final_list_values = []
    for a in sub_list_values:
        if "select" in a:
            final_list_values.append("nvarchar(250)")
        elif a == "text":
            final_list_values.append("nvarchar(MAX)")
        elif a == "string":
            final_list_values.append("nvarchar(MAX)")
        elif a == "boolean":
            final_list_values.append("nchar(5)")
        elif a == "phoneNumber":
            final_list_values.append("nvarchar(100)")
        elif a == "date":
            final_list_values.append("date")
        elif a == "time":
            final_list_values.append("time")
        elif a == "datetime":
            final_list_values.append("datetime")
        elif a == "integer":
            final_list_values.append("int")
        elif a == "decimal":
            final_list_values.append("float")
        elif a == "gpsLocation":
            final_list_values.append("nvarchar(100)")
        elif a == "email":
            final_list_values.append("nvarchar(100)")
        else:
            final_list_values.append("nvarchar(MAX)")
    return final_list_values

# approach == 1 will create sql table with nvarchar(MAX) dtypes for all columns
# approach == 2 will create sql table with specified dtypes from DM form schema
# function to prepare SQL statement string for file:
def create_tbl_statement(db_name, tbl_name, approach):
    if approach == 1:
        column_name = cleaning_keys()
        column_data_type = "nvarchar(MAX)"
        create_tbl = f'use {db_name}; CREATE TABLE {tbl_name} ('
        for x in column_name:
            create_tbl = create_tbl + x + ' ' + column_data_type + ' ' + 'NULL' + ',' + ' '
        create_tbl = create_tbl[:-1] + ' );'
        return create_tbl
    elif approach == 2:
        column_name = cleaning_keys()
        column_data_type = dtypes_prep()
        create_tbl = f'use {db_name}; CREATE TABLE {tbl_name} ('
        for x, y in zip(column_name, column_data_type):
            create_tbl = create_tbl + x + ' ' + y + ' ' + 'NULL' + ',' + ' '
        create_tbl = create_tbl[:-1] + ' );'
        return create_tbl
    else:
        print("Wrong approach, try either 1 or 2!")

# function to save prepared sql statement into the file:
def save_file():

    ####################################################################
    # Parameters => database name, sql table name and approach (1 or 2)!
    main_string = create_tbl_statement("db",
                                       "tbl",
                                       2)
    # Parameters => database name, sql table name and approach (1 or 2)!
    ####################################################################

    last_comma_index = main_string.rfind(",")
    output_string = main_string[:last_comma_index].rstrip() + ");"
    with open("ready_for_sql_new.txt", "w") as f:
            f.write(output_string.replace(",", ".,").
                    replace(",", "\n").
                    replace(".", ",").
                    replace(";", ";^*").
                    replace("*", "\n").
                    replace("^", "\n").
                    rstrip())

# function to open the file:
def open_file():
    save_file()
    time.sleep(1)
    pname = "notepad.exe"
    fname = "ready_for_sql_new.txt"
    sp.Popen([pname, fname])

# calling the final function:
open_file()