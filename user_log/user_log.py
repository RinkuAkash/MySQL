import mysql.connector
import datetime

# db_connection connects mysql server of root user and uses database of ANALYSIS
db_connection = mysql.connector.connect(host='localhost', user='root', passwd='root', database='ANALYSIS')
cursor = db_connection.cursor()


# import_data function loads csv file into database
def import_data():
    query = """LOAD DATA INFILE 'CpuLogData2019-10-24.csv' INTO TABLE user_log FIELDS TERMINATED BY ',' ENCLOSED BY 
    '"' LINES TERMINATED BY '""" + r'\n' + """' IGNORE 1 ROWS; """
    cursor.execute(query)
    db_connection.commit()


# this function is used to store idle, working , start and end time of user cpu data
def get_idle_and_working_hours():
    query = """SELECT DateTime, keyboard, mouse , user_name FROM user_log WHERE DateTime >= '2019-10-24 08:30:00' AND 
    DateTime <= '2019-10-24 19:30:00' """
    cursor.execute(query)
    # fetched data contains query data
    fetched_data = cursor.fetchall()
    cursor.execute('SELECT user_name FROM job')
    unique_users = dict()
    # user in cursor contain unique users which is stored in job table
    for user in cursor:
        unique_users[user[0]] = {'idle_time': datetime.datetime(2019, 10, 24, 0, 0, 0),
                                 'working_hours': datetime.datetime(2019, 10, 24, 0, 0, 0),
                                 'start_time': None,
                                 'end_time': datetime.datetime(2019, 10, 24, 8, 30, 00)}

    count_idle = 0
    # row is every of fetched data that DateTime, keyboard, mouse and user_name
    for row in fetched_data:
        # checking idle time, if exceeded 30min add to idle time of user dictionary
        if count_idle >= 5:
            if count_idle == 5:
                unique_users[row[3]]['idle_time'] = unique_users[row[3]].get('idle_time') + datetime.timedelta(0, 300)
            else:
                unique_users[row[3]]['idle_time'] = unique_users[row[3]].get('idle_time') + datetime.timedelta(0, 50)
        if row[1] == 0 and row[2] == 0:
            count_idle += 1
        else:
            if unique_users[row[3]]['start_time'] is None:
                unique_users[row[3]]['start_time'] = row[0]
            if unique_users[row[3]]['end_time'] < row[0]:
                unique_users[row[3]]['end_time'] = row[0]
            unique_users[row[3]]['working_hours'] = unique_users[row[3]].get('working_hours') + datetime.timedelta(0,
                                                                                                                   300)
            count_idle = 0

    # updating data calculated that working, idle hours and start, end times into database
    for row in unique_users:
        query = "UPDATE job SET idle_hours='{}', working_hours ='{}', start_time = '{}', end_time = '{}'  WHERE " \
                "user_name = '{}'".format(unique_users[row]['idle_time'], unique_users[row]['working_hours'],
                                          unique_users[row]['start_time'], unique_users[row]['end_time'], row)
        cursor.execute(query)
        db_connection.commit()


def find_lowest_average_hours():
    query = 'SELECT user_name FROM job WHERE working_hours < (SELECT avg(working_hours) FROM job)'
    cursor.execute(query)
    users = cursor.fetchall()
    for user in users:
        print(user[0])
    print(len(users))


def find_highest_average_hours():
    query = 'SELECT user_name FROM job WHERE working_hours > (SELECT avg(working_hours) FROM job)'
    cursor.execute(query)
    users = cursor.fetchall()
    for user in users:
        print(user[0])
    print(len(users))


def find_late_comers():
    query = "SELECT user_name FROM job WHERE start_time > '2019-10-24 09:30:00'"
    cursor.execute(query)
    total_late_comers = 0
    for user in cursor:
        total_late_comers += 1
        print(user[0])
    print(total_late_comers)


def find_highest_idle_hours():
    query = 'SELECT user_name FROM job WHERE time(idle_hours) > (select avg(time(idle_hours)) from job)'
    cursor.execute(query)
    total_users = 0
    for user in cursor:
        print(user[0])
        total_users += 1
    print(total_users)


if __name__ == '__main__':
    while True:
        print("1. lowest average hours\n2. highest average hours\n3. late comers\n4. highest idle hours\n 0. exit")
        option = int(input())

        if option == 1:
            find_lowest_average_hours()
        elif option == 2:
            find_highest_average_hours()
        elif option == 3:
            find_late_comers()
        elif option == 4:
            find_highest_idle_hours()
        elif option == 0:
            break
        else:
            print("Invalid")