import hashlib
import json
import pandas as pd
import os
import sqlite3

# Dict to store the unencrypted and encrypted pairs as key-value pairs.
anon_dict = {}

# Function to make a md5 string from a string
def getMd5(item):
    val = str(item)
    return hashlib.md5(val.encode()).hexdigest()

# Find the path where this script is located
base_directory = os.path.dirname(os.path.realpath(__file__))

# Path inside the base directory
path = "/Data/CDRdata_20211116_kurz.xlsx"
# path = "/Data/CDRdata_20211116.xlsx"

# Read the CSV data from the telefone-log-file
df_org = pd.read_excel(base_directory+path)

print(df_org)

# Anonymize the columnes that are critical and store the hash in a dict for later investigations
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
df_anon = df_org.apply(anonColumns, axis="columns")

# Drop the columnes TN-Name and Name
df_anon.drop('TN-Name', axis="columns", inplace=True)
df_anon.drop('Name', axis="columns", inplace=True)

# Create or open an existing file and write a dict in the file
with open('Data/json_anon.json', 'w') as outfile:
    json.dump(anon_dict, outfile)

# df_anon.to_excel(excel_writer= base_directory+ "/Dataset_anonymized.xlsx",startcol=0,startrow=0)

# Write the dataframe "df_anon" to the database
con = sqlite3.connect('db_anon.db')

df_anon.to_sql(name='df_anon', con=con)


print(df_anon)
