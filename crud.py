# import pandas to access csv files
import pandas as pd
# import numpy to compare data-frame types in creation of tables
import numpy as np

# import mysql.connector to connect mysql server and access database
import mysql.connector
# import Flask to access http, routing and debug
# import request to handle url parameter
# import jsonify to convert raw data into json format
from flask import Flask, request, jsonify
# import mysql.connector to connect mysql server and access database
import mysql.connector

# configuring Flask application to app
app = Flask(__name__)
# connecting to mysql server with database
connection = mysql.connector.connect(host='localhost', user='root',
                                     passwd='root', database='TechSolutions')
# creating cursor to handle query execution and fetching data
cursor = connection.cursor()


def get_statement(file):
    data_frame = pd.read_csv(file)

    # headers contains columns of csv file
    headers = data_frame.columns
    # size list to store max value for each column
    size = []
    # type list to store each data type
    type_list = []
    for label in headers:
        type = data_frame[label].dtype
        if type == np.int64:
            type_list.append('bigint')
            size.append(len(str(max(data_frame[label]))))
        elif type == np.float64:
            type_list.append('varchar')
            size.append(len(str(max(data_frame[label]))))
        else:
            type_list.append('varchar')
            size.append(int(data_frame[label].str.len().max()))
    statement = 'create table csv_file ( '

    for index in range(len(headers)):
        statement = (statement + "\n {} {}({}),".format(headers[index].lower(), type_list[index], size[index]+20))

    statement = statement[:-1] + ')'
    return statement


# index page which displays rest operations
@app.route("/")
def index():
    return "<h2>Welcome TechSolutions employees CRUD operations</h2>" \
           "<p>/tables : To checks tables</p> " \
           "<p>/employees : To check list of employees</p>" \
           "<p>/add_employee?name={employee_name}&designation={role}" \
           "&salary={amount} : To add employee into database</p>" \
           "<p>/delete?type={column_name}&value={row_value} : To delete value</p>" \
           "<p>/update?update_type={change_type}&update_value={change_value}&" \
           "column_type={column_name}&row_value={row_value} : To update</p>" \
           "<p>/import?file={file_name} : To import data from csv file</p>" \
           "<p>/export?file={file_path} : To export data from database</p>"


# tables function to display number of tables that present in database
@app.route("/tables")
def tables():
    cursor.execute("show tables")
    table = cursor.fetchall()
    return jsonify(table)


# employees route to show employees present in database
@app.route("/employees")
def show_employees():
    cursor.execute("select * from employees")
    employees = cursor.fetchall()
    return jsonify(employees)


# insert operation to add employee into database
@app.route("/add_employee", methods=['GET'])
def add_employee():
    employee_name = request.args.get("name")
    designation = request.args.get("designation")
    salary = request.args.get("salary")
    try:
        cursor.execute("INSERT INTO employees(name, designation, salary)"
                       " values({},{},{})".format(employee_name,
                                                  designation, salary))
        connection.commit()
        return "New employee added successfully"
    except:
        return "Request unsuccessful"


# delete operation to delete employee from database
@app.route("/delete", methods=['GET'])
def delete_employee():
    delete_type = request.args.get("type")
    type_value = request.args.get("value")
    try:
        cursor.execute("DELETE FROM employees WHERE"
                       " " + delete_type + " = " + type_value)
        connection.commit()
        return "Deleted successfully"
    except:
        return "Deletion unsuccessful"


# update function to update or modify existing value in database
@app.route("/update", methods=['GET'])
def update_data():
    update_type = request.args.get("update_type")
    update_value = request.args.get("update_value")
    column_type = request.args.get("column_type")
    row_value = request.args.get("row_value")
    try:
        cursor.execute("UPDATE employees SET " + update_type
                       + " = " + update_value
                       + " WHERE " + column_type
                       + " = " + row_value)
        connection.commit()

        return "Update successful"
    except:

        return "Update unsuccessful"


# importing data from csv file to database
@app.route("/import", methods=['GET'])
def import_data():
    file_name = request.args.get("file")
    query = get_statement(file_name)
    cursor.execute("DROP TABLE IF EXISTS csv_file")
    try:
        cursor.execute(query)
    except:
        return "Unable to create table"

    query = """LOAD DATA INFILE '""" + file_name + """' INTO TABLE csv_file FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
    LINES TERMINATED BY '""" + r'\n' + """' IGNORE 1 ROWS; """
    try:
        cursor.execute(query)
        connection.commit()
        return "Successfully imported"
    except:
        return "unable import into database"


# export operation to load data onto external file
@app.route("/export", methods=['GET'])
def export_data():
    file_name = request.args.get("file")
    query = """SELECT * FROM csv_file INTO OUTFILE '""" + file_name + """' FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
    LINES TERMINATED BY '""" + r'\r\n' + """'; """
    try:
        cursor.execute(query)
        connection.commit()
        return "Export success"
    except:

        return "Export failed"
