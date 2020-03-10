import mysql.connector
import pandas as pd
import os
import numpy as np

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="1@Safegns@2",
  database="prob2",
  auth_plugin="mysql_native_password") # Connect to database

my_cursor = mydb.cursor(buffered=True) #cursor to database, buffered=True to store multiple data in the buffer

tables=my_cursor.execute("show databases") 
'''
for db in my_cursor:
	print(db)
'''
my_cursor.execute("use prob2")
my_cursor.execute("show tables")
'''
for tables in my_cursor:
	print(tables)
'''
#for each xlsx file in the directory that we have download with acquire.py
for filename in os.listdir("./"):
	if filename=="page27.xlsx":
		continue
	#print(filename)
	if filename.endswith(".xlsx"):
		df = pd.read_excel(filename) #get excel file in a dataframe
		#print(df["Name"])

		#Each column in dataframe
		for i in range(len(df)): 
			name = df.loc[i,"Name"] #get Name of ith row 
			employer = df.loc[i,"Employer"]
			pension = df.loc[i,"Pension_Name"]
			title = df.loc[i,"Job Title"]
			pension_value = df.loc[i,"Pension_Amount"] 
			years = df.loc[i,"Years of  Service"]
			retire = df.loc[i,"Year of  Retirement"]
			#print(str(employer))
			if str(employer)=="nan": #some files contain no values for column so skip that row 
				continue
			
			if str(title)=="nan":
				continue
			
			res_query = "select ID from T_PERSON WHERE NAME=(%s)"#get ID of the person
			res_value = (name,)
			my_cursor.execute(res_query,res_value)
			person_id = my_cursor.fetchone()
			#print(person_id)
			if person_id==None: #if ID is not in the database that means we dont have that person in our database yet so insert it
				query = "INSERT INTO T_PERSON(NAME) VALUES (%s)"
				values = (name,)
				my_cursor.execute(query,values)
				mydb.commit()
				res_query = "select ID from T_PERSON WHERE NAME=(%s)" #Again get id for future use
				res_value = (name,)
				my_cursor.execute(res_query,res_value)
				person_id = my_cursor.fetchone()
				#print(person_id)
			
			#print(title,employer)

			res_query = "select ID from T_EMPLOYER WHERE NAME=(%s)"#get ID of employer
			res_value = (employer,)
			my_cursor.execute(res_query,res_value)
			employer_id = my_cursor.fetchone()
			#print(employer_id)
			if employer_id==None:#If ID is not in the database that means we dont have that employer in our database yet so insert it
				query = "INSERT INTO T_EMPLOYER(NAME) VALUES (%s)"
				values = (employer,)
				my_cursor.execute(query,values)
				mydb.commit()
				res_query = "select ID from T_EMPLOYER WHERE NAME=(%s)"# Again get ID for future use
				res_value = (employer,)
				my_cursor.execute(res_query,res_value)
				employer_id = my_cursor.fetchone()
				#print(person_id)

			
			query = "INSERT INTO T_EMPLOYMENT(TITLE,PERSON_ID,EMPLOYER_ID) VALUES (%s,%s,%s)"#populate T_EMPLOYMENT table with appropriate values
			values = (title,person_id[0],employer_id[0],)
			my_cursor.execute(query,values)
			mydb.commit()

			res_query = "select ID from T_PENSION WHERE NAME=(%s)"#get Pension ID
			res_value = (pension,)
			my_cursor.execute(res_query,res_value)
			pension_id = my_cursor.fetchone()
			#print(pension_id)
			if pension_id==None:#If ID is not in the database that means we dont have that pension in our database yet so insert it
				query = "INSERT INTO T_PENSION(NAME) VALUES (%s)"
				values = (pension,)
				my_cursor.execute(query,values)
				mydb.commit()
				res_query = "select ID from T_PENSION WHERE NAME=(%s)"#Again get ID for future use
				res_value = (pension,)
				my_cursor.execute(res_query,res_value)
				employer_id = my_cursor.fetchone()
				#print(pension_id)
				
			res_query = "select ID from T_EMPLOYMENT WHERE PERSON_ID=(%s) and EMPLOYER_ID=(%s)" #get EMPLOYMENT_ID to insert into PENSION_DETAILS table
			res_value = (person_id[0],employer_id[0],)
			my_cursor.execute(res_query,res_value)
			employment_id = my_cursor.fetchone()
			print(employment_id,pension_id[0])
			query = "INSERT INTO T_PENSION_DETAIL(EMPLOYMENT_ID,PENSION_ID,PENSION_VALUE,YEARS_OF_SERVICE,YEAR_OF_RETIREMENT) VALUES (%s,%s,%s,%s,%s)"# Populate using appropriate values
			values = (employment_id[0],pension_id[0],str(pension_value),str(years),str(retire))
			my_cursor.execute(query,values)
			mydb.commit()
			
print('Success')


