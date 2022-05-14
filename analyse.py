import pandas as pd
import sqlite3
import os
from datetime import datetime


# Filter the original db, so that we only have calls during the workinghours.
# Montag-Donnerstag: 8:00-12:00  13:30-17:00
# Freitag: 8:00-12:00  13:30-16:00.
def filter_workinghours(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # filter for all inbound/outbound calls during working hours
    def read_db(in_or_out):
        
        return pd.read_sql_query(f"""SELECT * FROM df_anon WHERE Gehend LIKE '{in_or_out}' AND 
                        (((Zeit BETWEEN '08:00:00' AND '12:00:00' OR Zeit BETWEEN '13:30:00' AND '17:00:00') AND
                        Wochentag != 'Samstag' AND Wochentag != 'Sonntag' AND Wochentag != 'Freitag') OR 
                        ((Zeit BETWEEN '08:00:00' AND '12:00:00' OR Zeit BETWEEN '13:30:00' AND '16:00:00') AND Wochentag = 'Freitag'))""", con)


    df_inbound = read_db('I')
    df_outbound = read_db('O')

    # Write the dataframe to the database: "df_working_hours_inbound"
    df_inbound.to_sql(name='df_working_hours_inbound', con=con, if_exists="replace")
    df_outbound.to_sql(name='df_working_hours_outbound', con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls during the daily hours
def amount_of_calls_during_hours(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)

    def get_in_or_outbound(in_or_out):
        # function to query the db and return a dataframe
        def check_hours_connected(start,end):
            return pd.read_sql_query(f"SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out} WHERE Zeit BETWEEN '{start}:00' AND '{end}:00' AND Verbunden = 1", con)
        def check_hours_lost(start,end):
            return pd.read_sql_query(f"SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out} WHERE Zeit BETWEEN '{start}:00' AND '{end}:00' AND Verloren = 1", con)
        
        # create a dictonary and call the function about with the given start and end time.
        dict_calls_per_hour = {
            "08:00 - 09:00": [check_hours_connected('08:00','09:00').shape[0],check_hours_lost('08:00','09:00').shape[0]],
            "09:00 - 10:00": [check_hours_connected('09:00','10:00').shape[0],check_hours_lost('09:00','10:00').shape[0]],
            "10:00 - 11:00": [check_hours_connected('10:00','11:00').shape[0],check_hours_lost('10:00','11:00').shape[0]],
            "11:00 - 12:00": [check_hours_connected('11:00','12:00').shape[0],check_hours_lost('11:00','12:00').shape[0]],
            "13:30 - 14:00": [check_hours_connected('13:30','14:00').shape[0],check_hours_lost('13:30','14:00').shape[0]],
            "14:00 - 15:00": [check_hours_connected('14:00','15:00').shape[0],check_hours_lost('14:00','15:00').shape[0]],
            "15:00 - 16:00": [check_hours_connected('15:00','16:00').shape[0],check_hours_lost('15:00','16:00').shape[0]],
            "16:00 - 17:00": [check_hours_connected('16:00','17:00').shape[0],check_hours_lost('16:00','17:00').shape[0]]
        }

        return dict_calls_per_hour
        

    # Inbound: change dict to df
    df_inbound = pd.DataFrame.from_dict(get_in_or_outbound('inbound'), orient='index')
    # name the columns
    df_inbound.columns =['Inbound_Verbunden',  "Inbound_Verloren"]
    # calc the ratio between lost and connected
    df_inbound["Inbound_Verhältnis"] = df_inbound["Inbound_Verbunden"]/df_inbound["Inbound_Verloren"]

    # Outbound: change dict to df
    df_outbound = pd.DataFrame.from_dict(get_in_or_outbound('outbound'), orient='index')
    # name the columns
    df_outbound.columns =['Outbound_Verbunden',  "Outbound_Verloren"]
    # calc the ratio between lost and connected
    df_outbound["Outbound_Verhältnis"] = df_outbound["Outbound_Verbunden"]/df_outbound["Outbound_Verloren"]

    # df = pd.merge(df_inbound, df_outbound, left_on='index', right_on='index')
    df = pd.concat([df_inbound, df_outbound], axis=1)

    # Write the dataframe to the database: "df_number_of_calls_during_hours_lost_connected_inbound / outbound"
    df.to_sql(name=f"df_number_of_calls_during_hours", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each weekday
def amount_of_calls_during_weekdays(db_path, in_or_out):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_days(day):
        return pd.read_sql_query(f"SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out} WHERE Wochentag = '{day}' ", con)
    
    # create a dictonary and call the function about with the given weekday.
    dict_calls_per_weekday = {
        "Montag": check_days('Montag').shape[0],
        "Dienstag": check_days('Dienstag').shape[0],
        "Mittwoch": check_days('Mittwoch').shape[0],
        "Donnerstag": check_days('Donnerstag').shape[0],
        "Freitag": check_days('Freitag').shape[0]
    }
    # change the dict to a dataframe
    df = pd.DataFrame.from_dict(dict_calls_per_weekday, orient='index')
    # name the columns
    df.columns =['Anzahl Total']
    # Write the dataframe to the database: "df_number_of_calls_during_weekdays_inbound / outbound"
    df.to_sql(name=f"df_number_of_calls_during_weekdays_{in_or_out}", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each month
def amount_of_calls_during_months(db_path, in_or_out):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_months(month):
        return pd.read_sql_query(f"SELECT Tag, Monat, Jahr FROM df_working_hours_{in_or_out} WHERE Monat = '{month}' ", con)
    
    # create a dictonary and call the function about with the given weekday.
    dict_calls_per_month = {
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
    df = pd.DataFrame.from_dict(dict_calls_per_month, orient='index')
    # name the columns
    df.columns =['Anzahl Total']
    # Write the dataframe to the database: "df_number_of_calls_during_months_inbound / outbound"
    df.to_sql(name=f"df_number_of_calls_during_months_{in_or_out}", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls each date of the whole year
def amount_of_calls_each_date(db_path, in_or_out):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def check_months(search_date):
        return pd.read_sql_query(f"SELECT Tag, Monat, Jahr FROM df_working_hours_{in_or_out} WHERE Datum = '{search_date}' ", con)
    
    # create a dictonary
    dict_calls_per_date = {}
    # start and end date for the date array function "pd.date_range"
    start_date = datetime(year=2021,month=4,day=28)
    end_date = datetime(year=2022,month=4,day=25)
    # pandas function to get an array of dates. freq='B' stands for Business so we only get the business days during that time.
    weekday_dates = pd.date_range(start=start_date, end=end_date, freq='B')

    # search for calls on each date from the above array
    for date in weekday_dates:
        dict_calls_per_date[date] = check_months(date).shape[0]

    # change the dict to a dataframe
    df = pd.DataFrame.from_dict(dict_calls_per_date, orient='index')
    # name the columns
    df.columns =['Anzahl Anrufe']
    # Write the dataframe to the database: "df_amount_of_calls_each_date"
    df.to_sql(name=f"df_amount_of_calls_each_date_{in_or_out}", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to count the number of calls from every number. (only incoming calls during working hours)
def amount_of_calls_from_same_number(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query the db and return a dataframe
    def get_df():
        return pd.read_sql_query(f"SELECT Rufnummer FROM df_working_hours_inbound", con)
    
    # create a Pandas.Series with values and their number of occurrences
    number_of_calls_per_tel_number = get_df().value_counts()
    # Create a df
    df = number_of_calls_per_tel_number.to_frame()
    # name the columns
    df.columns =['Anzahl Anrufe']
    # Write the dataframe to the database: "df_amount_of_calls_from_same_number"
    df.to_sql(name='df_amount_of_calls_from_same_number', con=con, if_exists="replace")
    # close the connection to the database
    con.close()