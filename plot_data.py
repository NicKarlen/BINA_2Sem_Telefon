import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import date

def plot_amount_of_calls_during_X(db_path, team=None):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_hours = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_hours", con)
    df_weekdays = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_weekdays", con)
    df_months = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_months", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with 6 seperate plots with no shared X axis
    fig, axes = plt.subplots(nrows=2, ncols=3, sharex=False)

    # Output the plots
    df_hours.plot.bar(x="index", y=["Inbound_Verbunden"]+["Inbound_Verloren"], ax=axes[0,0], xlabel='Tageszeiten', ylabel="Anzahl Eingehende Anrufe", legend=True) # , stacked=True
    df_hours.plot.bar(x="index", y=["Outbound_Verbunden"]+["Outbound_Verloren"], ax=axes[1,0], xlabel='Tageszeiten', ylabel="Anzahl Ausgehende Anrufe", legend=True)
    df_weekdays.plot.bar(x="index", y=["Inbound_Verbunden"]+["Inbound_Verloren"], ax=axes[0,1], xlabel='Wochentage', legend=True)
    df_weekdays.plot.bar(x="index", y=["Outbound_Verbunden"]+["Outbound_Verloren"], ax=axes[1,1], xlabel='Wochentage', legend=True)
    df_months.plot.bar(x="index", y=["Inbound_Verbunden"]+["Inbound_Verloren"], ax=axes[0,2], xlabel='Monate', legend=True)
    df_months.plot.bar(x="index", y=["Outbound_Verbunden"]+["Outbound_Verloren"], ax=axes[1,2], xlabel='Monate', legend=True)

    df_hours.plot(kind='line', x="index", y="Inbound_Lost_Percent",ax=axes[0,0], secondary_y=["Inbound_Lost_Percent"], color='green')
    axes[0,0].tick_params('x', labelrotation=90)
    df_hours.plot(kind='line', x="index", y="Outbound_Lost_Percent",ax=axes[1,0], secondary_y=["Outbound_Lost_Percent"], color='green')
    axes[1,0].tick_params('x', labelrotation=90)
    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(left=0.202,bottom=0.208,right=0.795,top=0.921,wspace=0.800,hspace=0.784) # -> for notebook
    # plt.subplots_adjust(left=0.202,bottom=0.148,right=0.795,top=0.921,wspace=0.179,hspace=0.476) # -> for desktop
    # Set main titel above all subplots
    if team == None:
        fig.suptitle("Auswertung der Ein- und Ausgehenden Anrufe während der Öffnungszeiten")
    else:
        fig.suptitle(f"Auswertung der Ein- und Ausgehenden Anrufe während der Öffnungszeiten ACHTUNG: Nur für Team: {team}")
    # Show the plot
    plt.show()    


def plot_amount_of_daily_calls(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_daily_calls = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_each_date", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with 2 seperate plots with shared X axis
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)

    # cut the hh:mm:ss from the index and make a new row "date"
    df_daily_calls['date'] = df_daily_calls['index'].str[:10]

    # Output the plots
    df_daily_calls.plot.bar(x="date", y=["Inbound_Verbunden", "Inbound_Verloren"], xlabel='Datum', 
                            ylabel="Anzahl Eingehende Anrufe", legend=True, ax=axes[0])

    df_daily_calls.plot.bar(x="date", y=["Outbound_Verbunden", "Outbound_Verloren"], xlabel='Datum', 
                            ylabel="Anzahl Ausgehende Anrufe", legend=True, ax=axes[1])

    # Only show every 5th tick label
    for i, t in enumerate(axes[1].get_xticklabels()):
        if (i % 5) != 0:
            t.set_visible(False)

    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.14, hspace=0.114)
    # Set titel
    fig.suptitle("Auswertung der ein- und ausgehenden, verbundenen oder verlorenen Anrufe während der Öffnungszeiten")
    # Show the plot
    plt.show()  


def plot_amount_of_calls_from_same_number(db_path):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_tel_numbers_inbound = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_from_same_number ", con)
    df_tel_numbers_outbound = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_from_same_number ORDER BY Outbound_Anzahl_Total DESC", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with 2 seperate plots with shared X axis
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=False)

    # Only take the first 150 rows
    df_short_inbound = df_tel_numbers_inbound.head(150)
    df_short_outbound = df_tel_numbers_outbound.head(150)

    # only take the first 7 characters from the index row and put it in to a new column
    df_short_inbound['short_index'] = df_short_inbound['index'].str[:7]
    df_short_outbound['short_index'] = df_short_outbound['index'].str[:7]

    # Output the plots
    df_short_inbound.plot.bar(x="short_index", y=["Inbound_Anzahl_Total"], xlabel='Rufnummer (ersten 7 Zeichen)', 
                            ylabel="Anzahl eingehende Anrufe pro Rufnummer", legend=False, ax=axes[0])

    df_short_outbound.plot.bar(x="short_index", y=["Outbound_Anzahl_Total"], xlabel='Rufnummer (ersten 7 Zeichen)', 
                            ylabel="Anzahl ausgehende Anrufe pro Rufnummer", legend=False, ax=axes[1])


    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.129, hspace=0.37, top=0.936)
    # Set titel
    fig.suptitle("Auswertung der ein- und ausgehenden Rufnummern während der Öffnungszeiten")
    # Show the plot
    plt.show()  

def plot_call_duration(db_path):
    """
        The call duration from the data makes no sense and will not be used in the analysis!
        A lot of call durations are negativ and some are more than 12h long?! (positiv and negativ?!)

    """
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_inbound = pd.read_sql_query(f"SELECT * FROM df_working_hours_inbound", con)
    # close the connection to the database
    con.close()
    # cut the hh:mm:ss from the index and make a new row "date"

    df_inbound['date'] = df_inbound['Datum'].str[:10]

    ax = df_inbound.plot(kind='scatter',x="date", y=["Rufdauer"], xlabel='date', 
                            ylabel="Rufdauer in Sekunden", legend=False)

    ax.set_yscale('log')

    # Only show every 5th tick label
    for i, t in enumerate(ax.get_xticklabels()):
        if (i % 5) != 0:
            t.set_visible(False)

    plt.xticks(rotation='vertical')

    # Set titel
    plt.title("Auswertung Rufdauer während der Öffnungszeiten")
    # Show the plot
    plt.show() 