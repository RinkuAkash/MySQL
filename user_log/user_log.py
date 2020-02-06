import mysql.connector
import pandas as pd
db_connection = mysql.connector.connect(host='localhost', user='root', passwd='root', database='ANALYSIS')
cursor = db_connection.cursor()


def import_data():
    query = """LOAD DATA INFILE 'CpuLogData2019-10-24.csv' INTO TABLE user_log FIELDS TERMINATED BY ',' ENCLOSED BY 
    '"' LINES TERMINATED BY '""" + r'\n' + """' IGNORE 1 ROWS; """
    cursor.execute(query)
    db_connection.commit()


