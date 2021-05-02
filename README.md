# Test_Solution

#Functionality
--> Read the file given by the Source team. 
--> Creates Staging,Reject tables based on the given Schema with Mandatory/Not NULL columns. Perform Data Validation to check D/H records and send all the rejected records to  Reject Table. Sends the correct records to Staging Table.
--> Create different Country tables based on the distinct countries from Staging table. Apply transformation based on the "Country" column on Staging table and insert data into different Country tables.

#Functions Used
create_connection():
	To make a connection to the DB Server.
create_database():
	To create a database in the local host.
connect_database():
	To connect to the created database in the local host.
query_exe():
	To execute the given SQL query in the database.
getcountries():
	To execute the given SQL query for fetching unique countries in the Staging Table.
create_table():
	To create a table if it does not exist in the database.
date()/date_rev():
	To transform date to the required format.
insert_table():
	To insert data into the tables in the database.
extract(): --Optional
	To extract data from a table to a delimited text file.
