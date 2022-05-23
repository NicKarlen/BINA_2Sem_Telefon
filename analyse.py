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

    # Call the query function for inbound and outbound calls
    df_inbound = read_db('I')
    df_outbound = read_db('O')

    # Clean up double entries from the reception (only needed for inbound calls)
    # There are always 4 entries for the reception.
    # If one call was connected then delete all the others, if no call is connected delete all but one. 
    df_inbound['similar'] = 0
    def delete_double_entries(row):
        try:
            if ((row['Zeit'] == df_inbound.loc[row.name+1,'Zeit'] and 
                row['Rufnummer'] == df_inbound.loc[row.name+1,'Rufnummer'] and
                row['Verbunden'] != 1) or
                (row['Zeit'] == df_inbound.loc[row.name-1,'Zeit'] and 
                row['Rufnummer'] == df_inbound.loc[row.name-1,'Rufnummer'] and
                df_inbound.loc[row.name-1,'Verbunden'] == 1) or
                (row['Zeit'] == df_inbound.loc[row.name-2,'Zeit'] and 
                row['Rufnummer'] == df_inbound.loc[row.name-2,'Rufnummer'] and
                df_inbound.loc[row.name-2,'Verbunden'] == 1) or
                (row['Zeit'] == df_inbound.loc[row.name-3,'Zeit'] and 
                row['Rufnummer'] == df_inbound.loc[row.name-3,'Rufnummer'] and
                df_inbound.loc[row.name-3,'Verbunden'] == 1)):
                row['similar'] = 1
            return row
        except:
            return row
    # call function above on every row
    df_inbound = df_inbound.apply(delete_double_entries, axis="columns")
    # drop all rows where 'similar' is equal 1
    df_inbound.drop(df_inbound[df_inbound.similar == 1].index, inplace=True)

    # Write the dataframe to the database: "df_working_hours_inbound"
    df_inbound.to_sql(name='df_working_hours_inbound', con=con, if_exists="replace")
    df_outbound.to_sql(name='df_working_hours_outbound', con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls during the daily hours
def amount_of_calls_during_hours(db_path, team=None):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query for the inbound and outbound data
    def get_in_or_outbound(in_or_out):
        # If no team is given
        if team == None:
            # function to query the db and return a dataframe
            def check_hours(start, end, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out}
                                             WHERE Zeit BETWEEN '{start}:00' AND '{end}:00' AND {connected_or_lost} = 1""", con)
        # If a team is given
        else:
            # function to query the db and return a dataframe
            def check_hours(start, end, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out} 
                                             WHERE Zeit BETWEEN '{start}:00' AND '{end}:00' AND {connected_or_lost} = 1 AND Team = '{team}'""", con)
        
        # create a dictonary and call the function about with the given start and end time and if we should look for lost or connected calls.
        dict_calls_per_hour = {
            "08:00 - 09:00": [check_hours('08:00','09:00', 'Verbunden').shape[0],check_hours('08:00','09:00', 'Verloren').shape[0]],
            "09:00 - 10:00": [check_hours('09:00','10:00', 'Verbunden').shape[0],check_hours('09:00','10:00', 'Verloren').shape[0]],
            "10:00 - 11:00": [check_hours('10:00','11:00', 'Verbunden').shape[0],check_hours('10:00','11:00', 'Verloren').shape[0]],
            "11:00 - 12:00": [check_hours('11:00','12:00', 'Verbunden').shape[0],check_hours('11:00','12:00', 'Verloren').shape[0]],
            "13:30 - 14:00": [check_hours('13:30','14:00', 'Verbunden').shape[0],check_hours('13:30','14:00', 'Verloren').shape[0]],
            "14:00 - 15:00": [check_hours('14:00','15:00', 'Verbunden').shape[0],check_hours('14:00','15:00', 'Verloren').shape[0]],
            "15:00 - 16:00": [check_hours('15:00','16:00', 'Verbunden').shape[0],check_hours('15:00','16:00', 'Verloren').shape[0]],
            "16:00 - 17:00": [check_hours('16:00','17:00', 'Verbunden').shape[0],check_hours('16:00','17:00', 'Verloren').shape[0]]
        }

        return dict_calls_per_hour
        

    # Inbound: change dict to df
    df_inbound = pd.DataFrame.from_dict(get_in_or_outbound('inbound'), orient='index')
    # name the columns
    df_inbound.columns =['Inbound_Verbunden',  "Inbound_Verloren"]
    # calc the lost percentage of the total number of inbound calls
    df_inbound["Inbound_Lost_Percent"] = (df_inbound["Inbound_Verloren"] / (df_inbound["Inbound_Verbunden"]+df_inbound["Inbound_Verloren"])) *100

    # Outbound: change dict to df
    df_outbound = pd.DataFrame.from_dict(get_in_or_outbound('outbound'), orient='index')
    # name the columns
    df_outbound.columns =['Outbound_Verbunden',  "Outbound_Verloren"]
    # calc the lost percentage of the total number of outbound calls
    df_outbound["Outbound_Lost_Percent"] = (df_outbound["Outbound_Verloren"] / (df_outbound["Outbound_Verbunden"]+df_outbound["Outbound_Verloren"])) * 100

    # concat the two dfs to one
    df = pd.concat([df_inbound, df_outbound], axis=1)

    # Write the dataframe to the database: "df_number_of_calls_during_hours_lost_connected_inbound / outbound"
    df.to_sql(name=f"df_number_of_calls_during_hours", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each weekday
def amount_of_calls_during_weekdays(db_path, team=None):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query for the inbound and outbound data
    def get_in_or_outbound(in_or_out):
        # If no team is given
        if team == None:
            # function to query the db and return a dataframe
            def check_days(day, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out}
                                             WHERE Wochentag = '{day}' AND {connected_or_lost} = 1 """, con)
        # If a team is given
        else:
            # function to query the db and return a dataframe
            def check_days(day, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Wochentag, Zeit FROM df_working_hours_{in_or_out} 
                                             WHERE Wochentag = '{day}' AND {connected_or_lost} = 1 AND Team = '{team}' """, con)           
        
        # create a dictonary and call the function about with the given weekday and lost/connected calls.
        dict_calls_per_weekday = {
            "Montag": [check_days('Montag', "Verbunden").shape[0], check_days('Montag', "Verloren").shape[0]],
            "Dienstag": [check_days('Dienstag', "Verbunden").shape[0], check_days('Dienstag', "Verloren").shape[0]],
            "Mittwoch": [check_days('Mittwoch', "Verbunden").shape[0], check_days('Mittwoch', "Verloren").shape[0]],
            "Donnerstag": [check_days('Donnerstag', "Verbunden").shape[0], check_days('Donnerstag', "Verloren").shape[0]],
            "Freitag": [check_days('Freitag', "Verbunden").shape[0], check_days('Freitag', "Verloren").shape[0]]
        }

        return dict_calls_per_weekday

    # change the dict to a dataframe
    df_inbound = pd.DataFrame.from_dict(get_in_or_outbound('inbound'), orient='index')
    # name the columns
    df_inbound.columns =['Inbound_Verbunden', 'Inbound_Verloren']

    # change the dict to a dataframe
    df_outbound = pd.DataFrame.from_dict(get_in_or_outbound('outbound'), orient='index')
    # name the columns
    df_outbound.columns =['Outbound_Verbunden', 'Outbound_Verloren']

    # concat the two dfs to one
    df = pd.concat([df_inbound, df_outbound], axis=1)


    # Write the dataframe to the database: "df_number_of_calls_during_weekdays"
    df.to_sql(name=f"df_number_of_calls_during_weekdays", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls on each month
def amount_of_calls_during_months(db_path, team=None):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # function to query for the inbound and outbound data
    def get_in_or_outbound(in_or_out):
        # If no team is given
        if team == None:
            # function to query the db and return a dataframe
            def check_months(month, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Tag, Monat, Jahr FROM df_working_hours_{in_or_out}
                                             WHERE Monat = '{month}' AND {connected_or_lost} = 1 """, con)
        # If a team is given
        else:
            def check_months(month, connected_or_lost):
                return pd.read_sql_query(f"""SELECT Tag, Monat, Jahr FROM df_working_hours_{in_or_out} 
                                             WHERE Monat = '{month}' AND {connected_or_lost} = 1 AND Team = '{team}' """, con)
        
        # create a dictonary and call the function about with the given months and if lost/connected calls.
        dict_calls_per_month = {
            "01": [check_months('1', "Verbunden").shape[0],check_months('1', "Verloren").shape[0]],
            "02": [check_months('2', "Verbunden").shape[0],check_months('2', "Verloren").shape[0]],
            "03": [check_months('3', "Verbunden").shape[0],check_months('3', "Verloren").shape[0]],
            "04": [check_months('4', "Verbunden").shape[0],check_months('4', "Verloren").shape[0]],
            "05": [check_months('5', "Verbunden").shape[0],check_months('5', "Verloren").shape[0]],
            "06": [check_months('6', "Verbunden").shape[0],check_months('6', "Verloren").shape[0]],
            "07": [check_months('7', "Verbunden").shape[0],check_months('7', "Verloren").shape[0]],
            "08": [check_months('8', "Verbunden").shape[0],check_months('8', "Verloren").shape[0]],
            "09": [check_months('9', "Verbunden").shape[0],check_months('9', "Verloren").shape[0]],
            "10": [check_months('10', "Verbunden").shape[0],check_months('10', "Verloren").shape[0]],
            "11": [check_months('11', "Verbunden").shape[0],check_months('11', "Verloren").shape[0]],
            "12": [check_months('12', "Verbunden").shape[0],check_months('12', "Verloren").shape[0]]
        }

        return dict_calls_per_month

    # change the dict to a dataframe
    df_inbound = pd.DataFrame.from_dict(get_in_or_outbound('inbound'), orient='index')
    # name the columns
    df_inbound.columns =['Inbound_Verbunden', 'Inbound_Verloren']

    # change the dict to a dataframe
    df_outbound = pd.DataFrame.from_dict(get_in_or_outbound('outbound'), orient='index')
    # name the columns
    df_outbound.columns =['Outbound_Verbunden', 'Outbound_Verloren']

    # concat the two dfs to one
    df = pd.concat([df_inbound, df_outbound], axis=1)

    # Write the dataframe to the database: "df_number_of_calls_during_months"
    df.to_sql(name=f"df_number_of_calls_during_months", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to extract the amount of calls each date of the whole year
def amount_of_calls_each_date(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)

    # start and end date for the date array function "pd.date_range"
    start_date = datetime(year=2021,month=4,day=28)
    end_date = datetime(year=2022,month=4,day=25)
    # pandas function to get an array of dates. freq='B' stands for Business so we only get the business days during that time.
    weekday_dates = pd.date_range(start=start_date, end=end_date, freq='B')

    # function to query for the inbound and outbound data
    def get_in_or_outbound(in_or_out):
        # function to query the db and return a dataframe
        def check_months(search_date,connected_or_lost):
            return pd.read_sql_query(f"SELECT Tag, Monat, Jahr FROM df_working_hours_{in_or_out} WHERE Datum = '{search_date}' AND {connected_or_lost} = 1", con)
        
        # create a empty dictonary
        dict_calls_per_date = {}

        # search for calls on each date from the weekday_dates array, for connected and lost calls
        for date in weekday_dates:
            dict_calls_per_date[date] = [check_months(date, 'Verbunden').shape[0], check_months(date, 'Verloren').shape[0]]

        return dict_calls_per_date


    # change the dict to a dataframe
    df_inbound = pd.DataFrame.from_dict(get_in_or_outbound('inbound'), orient='index')
    # name the columns
    df_inbound.columns =['Inbound_Verbunden', 'Inbound_Verloren']

    # change the dict to a dataframe
    df_outbound = pd.DataFrame.from_dict(get_in_or_outbound('outbound'), orient='index')
    # name the columns
    df_outbound.columns =['Outbound_Verbunden', 'Outbound_Verloren']

    # concat the two dfs to one
    df = pd.concat([df_inbound, df_outbound], axis=1)

    # Write the dataframe to the database: "df_amount_of_calls_each_date"
    df.to_sql(name=f"df_amount_of_calls_each_date", con=con, if_exists="replace")
    # close the connection to the database
    con.close()


# function to count the number of calls from every number. (only incoming calls during working hours)
def amount_of_calls_from_same_number(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    
    # function to query for the inbound and outbound data
    def get_in_or_outbound(in_or_out):
        # function to query the db and return a dataframe
        def get_df():
            return pd.read_sql_query(f"SELECT Rufnummer FROM df_working_hours_{in_or_out}", con)
        
        # create a Pandas.Series with values and their number of occurrences
        number_of_calls_per_tel_number = get_df().value_counts()
        # Create a df from the series object
        df = number_of_calls_per_tel_number.to_frame()

        return df

    # combine the two dataframes that we get from the function above (inbound/outbound)
    df = pd.concat([get_in_or_outbound('inbound'), get_in_or_outbound('outbound')], axis=1)

    # name the columns
    df.columns =['Inbound_Anzahl_Total', 'Outbound_Anzahl_Total']

    # Write the dataframe to the database: "df_amount_of_calls_from_same_number"
    df.to_sql(name='df_amount_of_calls_from_same_number', con=con, if_exists="replace")
    # close the connection to the database
    con.close()

