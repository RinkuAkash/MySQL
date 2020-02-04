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


# index page which displays rest operations
@app.route("/")
def index():
    return "<h2>Welcome TechSolutions employees CRUD operations</h2>" \
           "<p>/tables : To checks tables</p> " \
           "<p>/employees : To check list of employees</p>" \
           "<p>/add_employee?name={employee_name}&designation={role}" \
           "&salary={amount} : To add employee into database</p>" \
           "<p>/delete?type={column_name}&value={row_value} : To delete value"


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
def delete_enployee():
    delete_type = request.args.get("type")
    type_value = request.args.get("value")
    try:
        cursor.execute("DELETE FROM employees WHERE"
                       " "+delete_type+" = "+type_value)
        connection.commit()
        return "Deleted successfully"
    except:
        return "Deletion unsuccessful"
