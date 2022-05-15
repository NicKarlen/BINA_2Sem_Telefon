import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import date

def plot_amount_of_calls_during_X(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_hours = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_hours", con)
    df_weekdays = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_weekdays", con)
    df_months = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_months", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with tree seperate plots with no shared X axis
    fig, axes = plt.subplots(nrows=1, ncols=3, sharex=False)

    # Output the plots
    df_hours.plot.bar(x="index", y=["Inbound_Verbunden"]+["Inbound_Verloren"], ax=axes[0], xlabel='Tageszeiten', ylabel="Anzahl Eingehende Anrufe", legend=True)
    df_weekdays.plot.bar(x="index", y=["Inbound_Anzahl_Total"], ax=axes[1], xlabel='Wochentage', legend=True)
    df_months.plot.bar(x="index", y=["Inbound_Anzahl_Total"], ax=axes[2], xlabel='Monate', legend=True)
    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.3)
    # Set main titel above all subplots
    fig.suptitle("Auswertung der Eingehenden Anrufe während der Öffnungszeiten")
    # Show the plot
    plt.show()    


def plot_amount_of_daily_calls(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_daily_calls = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_each_date", con)
    # close the connection to the database
    con.close()

    # cut the hh:mm:ss from the index and make a new row "date"
    df_daily_calls['date'] = df_daily_calls['index'].str[:10]

    # Output the plots
    ax = df_daily_calls.plot.bar(x="date", y=["Inbound_Anzahl_Total"], xlabel='Datum', 
                            ylabel="Anzahl Eingehende Anrufe", legend=False)

    # Only show every 5th tick label
    for i, t in enumerate(ax.get_xticklabels()):
        if (i % 5) != 0:
            t.set_visible(False)

    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.3, left=0.038, right=0.976)
    # Set titel
    plt.title("Auswertung der Eingehenden Anrufe während der Öffnungszeiten")
    # Show the plot
    plt.show()  


def plot_amount_of_calls_from_same_number(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_tel_numbers = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_from_same_number", con)
    # close the connection to the database
    con.close()

    df_short = df_tel_numbers.head(150)
    # Output the plots
    ax = df_short.plot.bar(x="index", y=["Anzahl Anrufe"], xlabel='Rufnummer', 
                            ylabel="Anzahl Anrufe pro Rufnummer", legend=False)


    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.4, left=0.038, right=0.976)
    # Set titel
    plt.title("Auswertung der Eingehenden Anrufe während der Öffnungszeiten")
    # Show the plot
    plt.show()  