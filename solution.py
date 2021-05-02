import re
import mysql.connector as mysqlcon
from mysql.connector.errors import Error
import csv


def create_connection(host, user, password):
    try:
        sql_con = mysqlcon.connect(
            host=host,
            user=user,
            password=password
        )
    except Error as e:
        print(f'Error: {e}')
    return sql_con


def connect_database(host, user, password, database):
    try:
        sql_con = mysqlcon.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
    except Error as e:
        print(f'Error: {e}')
    return sql_con


def create_database(connect, query):
    cursor = connect.cursor()
    try:
        cursor.execute(query)
        connect.commit()
        cursor.close()
    except Error as e:
        print(f'Error: {e}')


def query_exe(connect, query):
    cursor = connect.cursor(buffered=True)
    try:
        cursor.execute(query)
        connect.commit()
        cursor.close()
        print("executed")
    except Error as e:
        print(f'Error: {e}')


# Get unique countries
def getcountries(connect, query):
    cursor = connect.cursor(buffered=True)
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        print("executed")
        return result
    except Error as e:
        print(f'Error: {e}')


# Connecting to mysql and creating an object
connect = create_connection("localhost", "root", "")


if connect:
    # Creating database as health_care if db doesnt exists
    create_db = create_database(
        connect, "CREATE DATABASE IF NOT EXISTS health_care")

# Connecting to mysql and creating an object
connect_db = connect_database("localhost", "root", "", "health_care")


def create_table(database_column_header, connect_db, name):
    param = ''
    for names in database_column_header:
        csv_file = csv.reader(open('Schema.csv', "r"), delimiter=",")
        for row in csv_file:
            if names == row[1]:
                if row[3] == 'DATE':
                    param += "{} {},".format(row[1], row[3])
                    break
                else:
                    if row[4] == 'Y':
                        if row[5] == 'Y':
                            param += "{} {}({}) PRIMARY KEY NOT NULL,".format(
                                row[1], row[3], row[2])
                            break
                        param += "{} {}({}) NOT NULL,".format(
                            row[1], row[3], row[2])
                        break
                    param += "{} {}({}),".format(row[1], row[3], row[2])
    query = "CREATE TABLE IF NOT EXISTS {} ({})".format(name, param[:-1])
    query_exe(connect_db, query)


def date(value):
    return value[:4]+'-'+value[4:6]+'-'+value[6:]


def date_rev(value):
    return value[4:]+'-'+value[2:4]+'-'+value[:2]


def insert_table(database_column_data, connect_db, table_name):
    param = ''
    database_column_data[2] = date(database_column_data[2])
    database_column_data[3] = date(database_column_data[3])
    database_column_data[8] = date_rev(database_column_data[8])
    for data in database_column_data:
        param += "'{}',".format(data)
    query = "Insert into {} values ({})".format(table_name, param[:-1])
    query_exe(connect_db, query)


file_names = open("filename.txt", "rt")
name_list = []
for name in file_names:
    name_list.append(name.strip())
file_names.close()

info_file = open(name_list[0], "r")
header_flag = False
for i in info_file:
    data = i.split("|")
    data = data[1:]
    data[-1] = data[-1].rstrip('\n')

    if data[0] == 'H' and not header_flag:
        database_column_header = data[1:]
        header_flag = True
        # creating table with the header data dynamically
        create_table(database_column_header, connect_db, 'Staging')
        create_table(database_column_header, connect_db, 'Staging_Reject')
        # print(database_column_header)

    elif data[0] == 'D' and header_flag:
        database_column_data = data[1:]
        insert_table(database_column_data, connect_db, "Staging")
        # print(database_column_data)
    else:
        insert_table(data[1:], connect_db, "Staging_reject")
        print("Rejected", data[1:])
info_file.seek(0)

country_code = []

result = getcountries(
    connect_db, "select distinct(Country) from Staging")
for i in result:
    for j in i:
        if j.isalpha():
            country_code.append(j)


for country in country_code:
    create_table(database_column_header, connect_db, country)

for i in info_file:
    data = i.split("|")
    data = data[1:]
    data[-1] = data[-1].rstrip('\n')
    if data[0] == 'D' and header_flag:
        database_column_data = data[1:]
        insert_table(database_column_data, connect_db, data[8])
info_file.close()


# def extract(connect, table_name):
#     cursor = connect.cursor(buffered=True)
#     try:
#         cursor.execute("select * from {}".format(table_name))
#         result = cursor.fetchall()
#         cursor.close()
#         print("executed")
#         return result
#     except Error as e:
#         print(f'Error: {e}')


# result = (extract(connect_db, 'Staging'))
# with open('extracted_data.csv', 'w') as f:

#     # using csv.writer method from CSV package
#     write = csv.writer(f, delimiter='|')

#     # write.writerow(fields)
#     write.writerows(result)
