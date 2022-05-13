import pandas as pd
import sqlite3
import os
from datetime import datetime


# Insert the DB name which is stored in the forlder Analyse/DB
db_name = "db_anon.db"
# Find the path where this script is located
base_directory = os.path.dirname(os.path.realpath(__file__))
# DB Path
db_path = f"{base_directory}/{db_name}"


# Filter the original db, so that we only have calls during the workinghours.
# Montag-Donnerstag: 8:00-12:00  13:30-17:00
# Freitag: 8:00-12:00  13:30-16:00.
def filter_workinghours():
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # filter for all incoming calls during working hours
    df = pd.read_sql_query("""SELECT * FROM df_anon WHERE Gehend LIKE 'I' AND 
                            (((Zeit BETWEEN '08:00:00' AND '12:00:00' OR Zeit BETWEEN '13:30:00' AND '17:00:00') AND
                            Wochentag != 'Samstag' AND Wochentag != 'Sonntag' AND Wochentag != 'Freitag') OR 
                            ((Zeit BETWEEN '08:00:00' AND '12:00:00' OR Zeit BETWEEN '13:30:00' AND '16:00:00') AND Wochentag = 'Freitag'))""", con)

    # print the amount of rows (each call is one row)
    print("Jährliche Anzahl von Anrufen während den Öffnungszeiten (Feiertagen nicht berücksichtigt):  ", df.shape[0])
    # Write the dataframe to the database: "df_working_hours"
    df.to_sql(name='df_working_hours', con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls during the daily hours
def amount_of_calls_during_hours():
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_hours(start,end):
        return pd.read_sql_query(f"SELECT Wochentag, Zeit FROM df_working_hours WHERE Zeit BETWEEN '{start}:00' AND '{end}:00' ", con)
    
    # create a dictonary and call the function about with the given start and end time.
    dict_calls_per_hour = {
        "08:00 - 09:00": check_hours('08:00','09:00').shape[0],
        "09:00 - 10:00": check_hours('09:00','10:00').shape[0],
        "10:00 - 11:00": check_hours('10:00','11:00').shape[0],
        "11:00 - 12:00": check_hours('11:00','12:00').shape[0],
        "13:30 - 14:00": check_hours('13:30','14:00').shape[0],
        "14:00 - 15:00": check_hours('14:00','15:00').shape[0],
        "15:00 - 16:00": check_hours('15:00','16:00').shape[0],
        "16:00 - 17:00": check_hours('16:00','17:00').shape[0]
    }
    # change the dict to a dataframe
    df = pd.DataFrame.from_dict(dict_calls_per_hour, orient='index')
    # print the dataframe
    print("Jährliche Anzahl von Anrufen während:", df)
    # Write the dataframe to the database: "df_number_of_calls_during_hours"
    df.to_sql(name='df_number_of_calls_during_hours', con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each weekday
def amount_of_calls_during_weekdays():
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_days(day):
        return pd.read_sql_query(f"SELECT Wochentag, Zeit FROM df_working_hours WHERE Wochentag = '{day}' ", con)
    
    # create a dictonary and call the function about with the given weekday.
    dict_calls_per_hour = {
        "Montag": check_days('Montag').shape[0],
        "Dienstag": check_days('Dienstag').shape[0],
        "Mittwoch": check_days('Mittwoch').shape[0],
        "Donnerstag": check_days('Donnerstag').shape[0],
        "Freitag": check_days('Freitag').shape[0]
    }
    # change the dict to a dataframe
    df = pd.DataFrame.from_dict(dict_calls_per_hour, orient='index')
    # print the dataframe
    print("Jährliche Anzahl von Anrufen während:", df)
    # Write the dataframe to the database: "df_number_of_calls_during_weekdays"
    df.to_sql(name='df_number_of_calls_during_weekdays', con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each month
def amount_of_calls_during_months():
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_months(month):
        return pd.read_sql_query(f"SELECT Tag, Monat, Jahr FROM df_working_hours WHERE Monat = '{month}' ", con)
    
    # create a dictonary and call the function about with the given weekday.
    dict_calls_per_hour = {
        "01": check_months('1').shape[0],
        "02": check_months('2').shape[0],
        "03": check_months('3').shape[0],
        "04": check_months('4').shape[0],
        "05": check_months('5').shape[0],
        "06": check_months('6').shape[0],
        "07": check_months('7').shape[0],
        "08": check_months('8').shape[0],
        "09": check_months('9').shape[0],
        "10": check_months('10').shape[0],
        "11": check_months('11').shape[0],
        "12": check_months('12').shape[0]
    }
    # change the dict to a dataframe
    df = pd.DataFrame.from_dict(dict_calls_per_hour, orient='index')
    # print the dataframe
    print("Jährliche Anzahl von Anrufen während Monat:", df)
    # Write the dataframe to the database: "df_number_of_calls_during_months"
    df.to_sql(name='df_number_of_calls_during_months', con=con, if_exists="replace")
    # close the connection to the database
    con.close()
