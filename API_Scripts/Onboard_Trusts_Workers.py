from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from aiohttp import ClientSession, ClientTimeout
from datetime import datetime
import os
import csv
import openpyxl
import pandas as pd
import numpy as np
import sqlite3 as sql
import matplotlib.pyplot as plt
import time
import pymysql
import difflib

# Please ensure that before you run this file you have a set of data to compare the PW data against. You can download this relevant data from a relevant Healthroster. 
# Please ensure that when you download this file that it contains NI_numbers, Names, Dates of Birth and the Hire date at the very least as all of these fields are required for this script to work.
# Please ensure that the downloaded file from Healthroster is named people.csv
# This script will attempt to access your local dockerised MYSQL database for Patchwork. Please check line 217 to ensure that all data matches for your database.


def get_pw_workers():
    PW_URL=os.environ["PW_URL"] # Graph QL endpoint live
    PW_JWT=os.environ['PW_JWT'] # PW JWT aimed at specific org. Need an admin account. 


    transport = AIOHTTPTransport(url=PW_URL, headers={'X-Authorization': PW_JWT})
    client = Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=180)
    print("Getting workers")
    workers_patchwork = []
    page_number=1
    while page_number >= 1:
            params = {"page": page_number}
            workers_query = gql(
                """
                query getWorkers($page: Int!){
                    workers(items: 100, page: $page) {
                        niNumber
                        firstName
                        lastName
                        esrNumber
                        dateOfBirth
                        id
                        numberOfShiftsWorked
                    }
                }
                """
            )
            result = client.execute(workers_query, variable_values=params)

            if result['workers'] == []:
                print("=========================================")
                print("No more pages at", page_number)
                print("=========================================")
                page_number = 0
                break

            print("Page number", page_number, "processed.")
            page_number += 1

            for worker in result['workers']:
                employee_number = worker['esrNumber']
                first_name = worker['firstName']
                last_name = worker['lastName']
                ni_number = worker['niNumber']
                date_of_birth = worker['dateOfBirth']
                worker_id = worker['id']
                shifts_worked = worker['numberOfShiftsWorked']
                workers_patchwork.append({'First Name': first_name,
                                        'Last Name': last_name,
                                        'employee_number': employee_number,
                                        'ni_number': ni_number,
                                        'date_of_birth': date_of_birth,
                                        'worker_id' : worker_id, 
                                        'shifts_worked' : shifts_worked})
                
    # New data frame set to list generated above 
    workers_patchwork = pd.DataFrame(workers_patchwork)
    # Saves data to same location as script
    workers_patchwork.to_csv('data_frame_logic.csv', index=False)


def highlight_name_difference(row):
    correct_name = row['hr_full_name']
    wrong_name = row['pw_full_name']
    differences = []

    for i, char in enumerate(correct_name):
        if i >= len(wrong_name) or char != wrong_name[i]:
            differences.append('^')
        else:
            differences.append(' ')
    return correct_name + '\n' + wrong_name + '\n' + ''.join(differences)

def highlight_dob_difference(row):
    correct_dob = row['Date of Birth']
    wrong_dob = row['date_of_birth']
    differences = []

    for i, char in enumerate(correct_dob):
        if i >= len(wrong_dob) or char != wrong_dob[i]:
            differences.append('^')
        else:
            differences.append(' ')
    return correct_dob + '\n' + wrong_dob + '\n' + ''.join(differences)



bad_hire_date_counter = 0
bad_esr_date_counter = 0
both_dates_bad_counter = 0

MY_FILE_PATH=os.environ['MY_FILE_PATH'] # Please ensure that your file path is both 1. Where this script is, and 2. Where your people.csv export from HR is.
MYSQL_PW=os.environ['MYSQL_PW'] # Password to your local MySQL DB

invalid_workers = []
# get_pw_workers()
# New data frame set to list generated above 
workers_patchwork = pd.read_csv(f"{MY_FILE_PATH}/data_frame_logic.csv")
# Sets up correct rows
workers_patchwork["pw_full_name"] = workers_patchwork['Last Name'] + " " + workers_patchwork['First Name']
workers_patchwork["amended_employee_number"] = workers_patchwork["employee_number"].str.split('-').str[0]

# New data frame set to converted csv from health roster
workers_health_roster = pd.read_csv(f"{MY_FILE_PATH}/people.csv")
# Sets up correct rows
workers_health_roster["amended_staff_number"] = workers_health_roster["Staff Number"].str.split('-').str[0]
workers_health_roster["hr_full_name"] = workers_health_roster['Surname'] + " " + workers_health_roster['Forenames']
workers_health_roster = workers_health_roster.rename(columns={'NINumber': '_ni_number'})
# Removes useless columns if exist
if 'Date Joned NHS' in workers_health_roster.columns:
    workers_health_roster = workers_health_roster.drop(['Date Joined NHS'], axis = 1)
if 'Contracted Time' in workers_health_roster.columns:
    workers_health_roster = workers_health_roster.drop(['Contracted Time'], axis = 1)
if 'Place of Birth' in workers_health_roster.columns:
    workers_health_roster = workers_health_roster.drop(['Place Of Birth'], axis =1)
if 'Current Unit' in workers_health_roster.columns:
    workers_health_roster = workers_health_roster.drop(['Current Unit'], axis=1)

workers_health_roster.to_csv(f"{MY_FILE_PATH}/people.csv", index=None, header=True)

# Merges both dataframes via NI_Number
df_merged_data = workers_health_roster.merge(workers_patchwork, left_on='_ni_number', right_on='ni_number')
df_merged_data_no_nan = df_merged_data.dropna(subset=['ni_number'])

df_merged_data_no_nan["SQL_command"] = "'" + df_merged_data_no_nan['ni_number'] + "',"
df_merged_data_no_nan["Date of Birth"] = pd.to_datetime(df_merged_data_no_nan["Date of Birth"]).dt.strftime('%Y-%m-%d')
df_merged_data_no_nan["Hire Date"] = pd.to_datetime(df_merged_data_no_nan['Hire Date']).dt.strftime('%Y-%m-%d')

#Mismatching data that has matching NI_Numbers. Names + DOB wrong
mismatched_df = df_merged_data_no_nan[(df_merged_data_no_nan['pw_full_name'] != df_merged_data_no_nan['hr_full_name']) | (df_merged_data_no_nan['date_of_birth'] != df_merged_data_no_nan['Date of Birth']) 
                                      | (df_merged_data_no_nan['Forenames'] != df_merged_data_no_nan['First Name']) | (df_merged_data_no_nan['Surname'] != df_merged_data_no_nan['Last Name'])]

#We will be using HR as the soruce of truth here 
mismatched_df["Mismatched_data"] = np.where(
    (mismatched_df['Forenames'] != mismatched_df['First Name']) & (mismatched_df['Date of Birth'] != mismatched_df['date_of_birth']), 
    'First name and dob mismatch',
    np.where(
    (mismatched_df['Forenames'] != mismatched_df['First Name']) & (mismatched_df['Surname'] != mismatched_df['Last Name']),
        'First name and last name mismatch',
        np.where(
            (mismatched_df['Forenames'] != mismatched_df['First Name']) & (mismatched_df['Surname'] != mismatched_df['Last Name']) & (mismatched_df['Date of Birth'] != mismatched_df['date_of_birth']), 
            'First name and last name and dob mismatch',
                np.where(
                    mismatched_df['Date of Birth'] != mismatched_df['date_of_birth'], 
                    'DOB mismatch',
                    np.where(
                        mismatched_df['Forenames'] != mismatched_df['First Name'], 
                        'First Name mismatch',
                        np.where(
                            mismatched_df['Surname'] != mismatched_df['Last Name'], 
                            'Last Name mismatch',
                            np.where(
                                (mismatched_df['Surname'] != mismatched_df['Last Name']) & (mismatched_df['Date of Birth'] != mismatched_df['date_of_birth']), 
                                'Last name and dob mismatch', 
                                ''
                            )
                        )
                    )
                )
            )
        )   
    )



mismatched_df['Update Query'] = ''
for index, row in mismatched_df.iterrows():
    if row['Mismatched_data'] == 'First name and last name mismatch':
        query = "UPDATE users SET first_name = '{}', last_name = '{}' WHERE id = '{}'".format(row['Forenames'], row['Surname'], row['worker_id'])
    elif row['Mismatched_data'] == 'DOB mismatch':
        query = "UPDATE users SET date_of_birth = '{}' WHERE id = '{}'".format(row['Date of Birth'], row['worker_id'])
    elif row['Mismatched_data'] == 'First name and dob mismatch':
        query = "UPDATE users SET first_name = '{}', date_of_birth = '{}' WHERE id = '{}'".format(row['Forenames'], row['Date of Birth'], row['worker_id']) 
    elif row['Mismatched_data'] == 'First name, last name and dob mismatch':
        query = "UPDATE users SET first_name = '{}', last_name = '{}', date_of_birth = '{}' WHERE id = '{}'".format(row['Forenames'], row['Surname'], row['Date of Birth'], row['worker_id'])
    elif row['Mismatched_data'] == 'First Name mismatch':
        query = "UPDATE users SET first_name = '{}' WHERE id = '{}'".format(row['Forenames'], row['worker_id'])
    elif row['Mismatched_data'] == 'Last Name mismatch':
        query = "UPDATE users SET last_name = '{}' WHERE id = '{}'".format(row['Surname'], row['worker_id'])
    elif row['Mismatched_data'] == 'Last name and dob mismatch':
        query = "UPDATE users SET last_name = '{}', date_of_birth = '{}' WHERE id = '{}'".format(row['Surname'], row['Date of Birth'], row['worker_id'])
    elif row['Mismatched_data'] == 'NaN':
        query = "No mismatches"
    else:
        continue
    mismatched_df.at[index, 'Update Query'] = query

mismatched_df['Differences'] = mismatched_df.apply(highlight_name_difference, axis=1)
mismatched_df['DOB_Differences'] = mismatched_df.apply(highlight_dob_difference, axis=1)

mismatched_df.to_csv(f"{MY_FILE_PATH}/mismatched_data.csv", index=None, header=True)

#Drops all data in mismatched data from df_merged_data_no_nan; ensures only data in this df matches on NI_number + DOB + Names
df_merged_data_no_nan = pd.merge(df_merged_data_no_nan, mismatched_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
df_merged_data_no_nan.to_csv(f"{MY_FILE_PATH}/merged_data_no_nan.csv", index=None, header=True)

conn = pymysql.connect(host='127.0.01', port=3306, user='root', password=MYSQL_PW, db='locumtap_production', unix_socket = None)
for index, row in mismatched_df.iterrows():
    hire_date = row['Hire Date']
    worker_id = row['worker_id']

    #SQL queries
    query = f"SELECT last_name, first_name, id, date(created_at) AS created_at FROM users WHERE '{hire_date}' > (SELECT MAX(created_at) FROM users WHERE id = {worker_id}) AND id = {worker_id};"
    esr_query = f"SELECT valid_from FROM esr_numbers WHERE '{hire_date}' > (SELECT MAX(valid_from) FROM esr_numbers where user_id = {worker_id}) AND user_id = {worker_id};"
    double_query = f"SELECT u.last_name, u.first_name, u.id, date(u.created_at) AS created_at, e.max_valid_from FROM users u JOIN ( SELECT user_id, MAX(valid_from) AS max_valid_from FROM esr_numbers GROUP BY user_id ) e ON u.id = e.user_id WHERE '{hire_date}' > (SELECT MAX(u2.created_at) FROM users u2 WHERE u2.id = {worker_id}) AND u.id = {worker_id} AND e.max_valid_from < '{hire_date}';"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        counter = 0
        if result:
            date = result[3]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            print(f"The created_at date on Patchwork is {date_str}")
            print(f"MISMATCHING WORKER: Worker ID {worker_id} was hired before {hire_date} (this date is the hire date from HR) on Patchwork")
            print("==========================================")
            bad_hire_date_counter += 1
        cursor.execute(esr_query)
        result = cursor.fetchone()
        if result:
            date = result[0]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            print(f"The latest valid from date on Patchwork is {date_str}") 
            print(f"MISMATCHING WORKER: Worker ID {worker_id} has an esr valid from date before {hire_date} on Patchwork")
            print("==========================================")
            bad_esr_date_counter += 1
        cursor.execute(double_query)
        result = cursor.fetchone()
        if result:
            date = result[3]
            valid_from_date = result[4]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            valid_from_date_str = datetime.strftime(valid_from_date, '%Y-%m-%d') 
            print("! --------------------- Both values are bad --------------------- ! ")
            print(f"The created_at date on Patchwork is: {date_str}")
            print(f"The MAX esr_numbers.valid_from date is: {valid_from_date_str}")
            print(f"MATCHING WORKER: Worker ID {worker_id} was hired before {hire_date} (this date is the hire date from HR) on Patchwork and their latest esr_valid from date is before {hire_date}")
            print("==========================================")
            both_dates_bad_counter += 1
            invalid_workers.append(f"MISMATCHED WORKER ID: {worker_id}")

print(f"MISMATCHING WORKERS SUMMARY: Total number of bad hire_dates = {bad_hire_date_counter}, Total number of bad valid_from dates = {bad_esr_date_counter}, Total number of entries where both valid_from and created_at dates are bad = {both_dates_bad_counter}")

bad_hire_date_counter = 0
bad_esr_date_counter = 0
both_dates_bad_counter = 0

for index, row in df_merged_data_no_nan.iterrows():
    hire_date = row['Hire Date']
    worker_id = row['worker_id']
    
    #SQL queries
    query = f"SELECT last_name, first_name, id, date(created_at) AS created_at FROM users WHERE '{hire_date}' > (SELECT MAX(created_at) FROM users WHERE id = {worker_id}) AND id = {worker_id};"
    esr_query = f"SELECT valid_from FROM esr_numbers WHERE '{hire_date}' > (SELECT MAX(valid_from) FROM esr_numbers where user_id = {worker_id}) AND user_id = {worker_id};"
    double_query = f"SELECT u.last_name, u.first_name, u.id, date(u.created_at) AS created_at, e.max_valid_from FROM users u JOIN ( SELECT user_id, MAX(valid_from) AS max_valid_from FROM esr_numbers GROUP BY user_id ) e ON u.id = e.user_id WHERE '{hire_date}' > (SELECT MAX(u2.created_at) FROM users u2 WHERE u2.id = {worker_id}) AND u.id = {worker_id} AND e.max_valid_from < '{hire_date}';"
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        counter = 0
        if result:
            date = result[3]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            print(f"The created_at date on Patchwork is {date_str}")
            print(f"MATCHING WORKER: Worker ID {worker_id} was hired before {hire_date} (this date is the hire date from HR) on Patchwork")
            print("==========================================")
            bad_hire_date_counter += 1
        cursor.execute(esr_query)
        result = cursor.fetchone()
        if result:
            date = result[0]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            print(f"The latest valid from date on Patchwork is {date_str}") 
            print(f"MATCHING WORKER: Worker ID {worker_id} has an esr valid from date before {hire_date} on Patchwork")
            print("==========================================")
            bad_esr_date_counter += 1
        cursor.execute(double_query)
        result = cursor.fetchone()
        if result:
            date = result[3]
            valid_from_date = result[4]
            date_str = datetime.strftime(date, '%Y-%m-%d')
            valid_from_date_str = datetime.strftime(valid_from_date, '%Y-%m-%d') 
            print("! --------------------- Both values are bad --------------------- ! ")
            print(f"The created_at date on Patchwork is: {date_str}")
            print(f"The MAX esr_numbers.valid_from date is: {valid_from_date_str}")
            print(f"MATCHING WORKER: Worker ID {worker_id} was hired before {hire_date} (this date is the hire date from HR) on Patchwork and their latest esr_valid from date is before {hire_date}")
            print("==========================================")
            both_dates_bad_counter += 1
            invalid_workers.append(f"MATCHED WORKER ID: {worker_id}")
 

print(f"MATCHING WORKERS SUMMARY: Total number of bad hire_dates = { bad_hire_date_counter }, Total number of bad valid_from dates = { bad_esr_date_counter }, Total number of entries where both valid_from and created_at dates are bad = {both_dates_bad_counter}")
print("Below please find workers that we will not be able to onboard currently. Hire Date and Valid_From date are both prior to the Healthroster Hire Date")
for element in invalid_workers:
    print(element)

print(f"Number of workers where NI matches between PW and HR but data is wrong; { len(mismatched_df) }")
print(f"Number of workers where NI matches between PW and HR but data is correct; { len(df_merged_data_no_nan) }")