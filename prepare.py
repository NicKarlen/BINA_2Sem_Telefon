import hashlib
import json
from numpy import NaN
import pandas as pd
import os
import sqlite3
from datetime import datetime

def prep():
    print("Prep is running - Time: ",datetime.now())

    # Dict to store the unencrypted and encrypted pairs as key-value pairs.
    anon_dict = {}

    # Find the path where this script is located
    base_directory = os.path.dirname(os.path.realpath(__file__))

    # Paths for excelsheets in the base directory
    # path_recordings = "/Data/CDRdata_20211116_kurz.xlsx"
    path_recordings = "/Data/20220426 Telefonjournal_J_AUL_FIM_28.04.2021.xlsx"
    path_team_list = "/Data/Team-Versionen.xlsx"


    # Read the excel data from the recordings-file
    df_org = pd.read_excel(base_directory+path_recordings)
    # Read the excel data from the teams-file (but the second sheet)
    xls = pd.ExcelFile(base_directory+path_team_list)
    df_teams = pd.read_excel(xls, 'Team inkl. Version')

    # Insert two rows in the df_org table that give information about the team
    df_org['Team'] = NaN
    df_org['Team inkl. Version'] = NaN

    # Function to assign the Team to every TN-Nr.
    def adjustTable(row):

        # Clean up the Rufnummer
        if len(str(row["Rufnummer"])) >= 9:
            row["Rufnummer"] = "0" + str(row["Rufnummer"])[-9:]
        # Clean up the Durchwahl-Nr.
        if len(str(row["Durchwahl-Nr."])) >= 9:
            row["Durchwahl-Nr."] = "0" + str(row["Durchwahl-Nr."])[-9:]

        # Change the TN-Nr. to int for further comparison
        if str(row["TN-Nr."]) != "nan":
            row["TN-Nr."] = int(row["TN-Nr."])

        # Assign the Team to every TN-Nr. (in the given period)
        tn_number = row['TN-Nr.']
        tn_date = row["Datum"]
        for i in range(0,df_teams.shape[0]):
            if ((df_teams.loc[i,'Team-Mitglied 1'] == tn_number or
                df_teams.loc[i,'Team-Mitglied 2'] == tn_number or
                df_teams.loc[i,'Team-Mitglied 3'] == tn_number)
                and df_teams.loc[i,'von'] <= tn_date and df_teams.loc[i,'bis'] >= tn_date):

                row['Team'] = str(df_teams.loc[i,'Team inkl. Version'])[:3]
                row['Team inkl. Version'] = str(df_teams.loc[i,'Team inkl. Version']) 
        return row
    # Call the defined function (above) on every row.
    print("Function adjustTable started - Time: ",datetime.now())
    df_org = df_org.apply(adjustTable, axis="columns")

    # print(df_org.tail(10))

    # Function to make a md5 hash-string from a string
    def getMd5(item):
        val = str(item)
        return hashlib.md5(val.encode()).hexdigest()

    # Anonymize the columnes that are critical and store the "key:value"-pair as hash and actual value in a dict for later investigations
    def anonColumns(row):
        # TN-Nr.
        if str(row["TN-Nr."]) != "nan":
            md5 = getMd5(row["TN-Nr."])
            anon_dict[md5] = row["TN-Nr."]
            row["TN-Nr."] = md5

        # Rufnummer, except the one that start with AUL_ein or AUL_aus
        if not row["Rufnummer"] in ["AUL_aus", "AUL_ein"] and str(row["Rufnummer"]) != "nan":
            md5 = getMd5(row["Rufnummer"])
            anon_dict[md5] = row["Rufnummer"]
            row["Rufnummer"] = md5

        # Durchwahl-Nr.
        if str(row["Durchwahl-Nr."]) != "nan":
            md5 = getMd5(row["Durchwahl-Nr."])
            anon_dict[md5] = row["Durchwahl-Nr."]
            row["Durchwahl-Nr."] = md5

        # Umleitungsziel
        if str(row["Umleitungsziel"]) != "nan":
            md5 = getMd5(row["Umleitungsziel"])
            anon_dict[md5] = row["Umleitungsziel"]
            row["Umleitungsziel"] = md5

        return row
    # Call the defined function (above) on every row.
    print("Function anonColumns started - Time: ",datetime.now())
    df_anon = df_org.apply(anonColumns, axis="columns")

    # Drop the columnes TN-Name and Name
    df_anon.drop('TN-Name', axis="columns", inplace=True)
    df_anon.drop('Name', axis="columns", inplace=True)

    # Create or open an existing file and write a dict in the file
    with open('Data/json_anon.json', 'w') as outfile:
        json.dump(anon_dict, outfile)

    # Write the dataframe "df_anon" to the database
    con = sqlite3.connect('db_anon.db')
    df_anon.to_sql(name='df_anon', con=con, if_exists="replace")
    con.close()

    print("Prep is done - Time: ",datetime.now())