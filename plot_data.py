import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import date

def plot_amount_of_calls_during_X(db_path, team='all', save=False):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_hours = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_hours WHERE Team = '{team}'", con)
    df_weekdays = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_weekdays WHERE Team = '{team}'", con)
    df_months = pd.read_sql_query(f"SELECT * FROM df_number_of_calls_during_months WHERE Team = '{team}'", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with 6 seperate plots with no shared X axis
    fig, axes = plt.subplots(nrows=2, ncols=3, sharex=False)

    # Output the plots
    df_hours.plot.bar(x="index",    y=["Inbound_Verbunden_intern"]+["Inbound_Verloren_intern"]+["Inbound_Verbunden_extern"]+["Inbound_Verloren_extern"],     ax=axes[0,0], xlabel='Tageszeiten', ylabel="Anzahl Eingehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=True, stacked=True)
    df_hours.plot.bar(x="index",    y=["Outbound_Verbunden_intern"]+["Outbound_Verloren_intern"]+["Outbound_Verbunden_extern"]+["Outbound_Verloren_extern"], ax=axes[1,0], xlabel='Tageszeiten', ylabel="Anzahl Ausgehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=True, stacked=True)
    df_weekdays.plot.bar(x="index", y=["Inbound_Verbunden_intern"]+["Inbound_Verloren_intern"]+["Inbound_Verbunden_extern"]+["Inbound_Verloren_extern"],     ax=axes[0,1], xlabel='Wochentage', ylabel="Anzahl Eingehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=False, stacked=True)
    df_weekdays.plot.bar(x="index", y=["Outbound_Verbunden_intern"]+["Outbound_Verloren_intern"]+["Outbound_Verbunden_extern"]+["Outbound_Verloren_extern"], ax=axes[1,1], xlabel='Wochentage', ylabel="Anzahl Eingehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=False, stacked=True)
    df_months.plot.bar(x="index",   y=["Inbound_Verbunden_intern"]+["Inbound_Verloren_intern"]+["Inbound_Verbunden_extern"]+["Inbound_Verloren_extern"],     ax=axes[0,2], xlabel='Monate', ylabel="Anzahl Eingehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=False, stacked=True)
    df_months.plot.bar(x="index",   y=["Outbound_Verbunden_intern"]+["Outbound_Verloren_intern"]+["Outbound_Verbunden_extern"]+["Outbound_Verloren_extern"], ax=axes[1,2], xlabel='Monate', ylabel="Anzahl Eingehende Anrufe", color=['lightgreen', 'lightblue', 'lightcoral', 'wheat'], legend=False, stacked=True)
    # Output the line chart on top of the bar chart with a secondary y-axis.
    df_hours.plot(kind='line', x="index", y="Inbound_Lost_Percent_intern",    ax=axes[0,0], xlabel='Tageszeiten', secondary_y=["Inbound_Lost_Percent_intern"], color='black', legend=True)
    df_hours.plot(kind='line', x="index", y="Outbound_Lost_Percent_intern",   ax=axes[1,0], xlabel='Tageszeiten', secondary_y=["Outbound_Lost_Percent_intern"], color='red', legend=True)
    df_weekdays.plot(kind='line', x="index", y="Inbound_Lost_Percent_intern", ax=axes[0,1], xlabel='Wochentage', secondary_y=["Inbound_Lost_Percent_intern"], color='black', legend=False)
    df_weekdays.plot(kind='line', x="index", y="Outbound_Lost_Percent_intern",ax=axes[1,1], xlabel='Wochentage', secondary_y=["Outbound_Lost_Percent_intern"], color='red', legend=False)
    df_months.plot(kind='line', x="index", y="Inbound_Lost_Percent_intern",   ax=axes[0,2], xlabel='Monate', secondary_y=["Inbound_Lost_Percent_intern"], color='black', legend=False)
    df_months.plot(kind='line', x="index", y="Outbound_Lost_Percent_intern",  ax=axes[1,2], xlabel='Monate', secondary_y=["Outbound_Lost_Percent_intern"], color='red', legend=False)
    # Output the line chart on top of the bar chart with a secondary y-axis.
    df_hours.plot(kind='line', x="index", y="Inbound_Lost_Percent_extern",    ax=axes[0,0], xlabel='Tageszeiten', secondary_y=["Inbound_Lost_Percent_extern"], color='red', legend=True)
    df_hours.plot(kind='line', x="index", y="Outbound_Lost_Percent_extern",   ax=axes[1,0], xlabel='Tageszeiten', secondary_y=["Outbound_Lost_Percent_extern"], color='black', legend=True)
    df_weekdays.plot(kind='line', x="index", y="Inbound_Lost_Percent_extern", ax=axes[0,1], xlabel='Wochentage', secondary_y=["Inbound_Lost_Percent_extern"], color='red', legend=False)
    df_weekdays.plot(kind='line', x="index", y="Outbound_Lost_Percent_extern",ax=axes[1,1], xlabel='Wochentage', secondary_y=["Outbound_Lost_Percent_extern"], color='black', legend=False)
    df_months.plot(kind='line', x="index", y="Inbound_Lost_Percent_extern",   ax=axes[0,2], xlabel='Monate', secondary_y=["Inbound_Lost_Percent_extern"], color='red', legend=False)
    df_months.plot(kind='line', x="index", y="Outbound_Lost_Percent_extern",  ax=axes[1,2], xlabel='Monate', secondary_y=["Outbound_Lost_Percent_extern"], color='black', legend=False)
    

    # for loop
    for i in range(3):
        for j in range(2):
            # Rotate the x label by 90 degree
            axes[j,i].tick_params('x', labelrotation=90)
            # Set ylabel of secondary axis
            axes[j,i].right_ax.set_ylabel('Percent')

    # Set y-axis limit  --> should be in the for loop on top!!! REFACTOR!
    axes[0,0].set_ylim((0 , max([axes[0,0].get_ylim()[1], axes[1,0].get_ylim()[1]])))
    axes[1,0].set_ylim((0 , max([axes[0,0].get_ylim()[1], axes[1,0].get_ylim()[1]])))
    axes[0,1].set_ylim((0 , max([axes[0,1].get_ylim()[1], axes[1,1].get_ylim()[1]])))
    axes[1,1].set_ylim((0 , max([axes[0,1].get_ylim()[1], axes[1,1].get_ylim()[1]])))
    axes[0,2].set_ylim((0 , max([axes[0,2].get_ylim()[1], axes[1,2].get_ylim()[1]])))
    axes[1,2].set_ylim((0 , max([axes[0,2].get_ylim()[1], axes[1,2].get_ylim()[1]])))

    # Adjust the legend position
    #axes[0,0].get_legend().set_bbox_to_anchor((0.45, 0.74))
    #axes[1,0].get_legend().set_bbox_to_anchor((0.47, 0.28))

    # Adjust the spacing at the bottom of the window
    # plt.subplots_adjust(left=0.098,bottom=0.208,right=0.929,top=0.921,wspace=0.554,hspace=0.784) # -> for notebook
    plt.subplots_adjust(left=0.080,bottom=0.148,right=0.929,top=0.921,wspace=0.345,hspace=0.476) # -> for desktop
    # Set main titel above all subplots
    if team == 'all':
        fig.suptitle("Auswertung der intern oder extern ein- und ausgehenden Anrufe während der Öffnungszeiten")
    else:
        fig.suptitle(f"Auswertung der intern oder extern ein- und ausgehenden Anrufe während der Öffnungszeiten ACHTUNG: Nur für Team: {team}")

    if save == False:
        # Show the plot
        plt.show()
    else:  
        # adjust the fig size for the saved image
        plt.gcf().set_size_inches(26, 10)
        # Save plot
        plt.savefig(f"Saved_Charts_Intern_Extern/calls_during_X_team_{team}.png", bbox_inches='tight' , dpi=300) 


def plot_amount_of_daily_calls(db_path, team='all', save=False):
    # Create a connection to the database
    con = sqlite3.connect(db_path)
    # query the db and return a dataframe
    df_daily_calls = pd.read_sql_query(f"SELECT * FROM df_amount_of_calls_each_date WHERE Team = '{team}'", con)
    # close the connection to the database
    con.close()

    # Create a plot-window with 2 seperate plots with shared X axis
    fig, axes = plt.subplots(nrows=4, ncols=1, sharex=True)

    # cut the hh:mm:ss from the index and make a new row "date"
    df_daily_calls['date'] = df_daily_calls['index'].str[:10]

    # Output the plots
    df_daily_calls.plot.bar(x="date", y=["Inbound_Verbunden_intern", "Inbound_Verloren_intern"], xlabel='Datum', 
                            ylabel="Anzahl eingehende\nAnrufe", legend=True, ax=axes[0])

    df_daily_calls.plot.bar(x="date", y=["Inbound_Verbunden_extern", "Inbound_Verloren_extern"], xlabel='Datum', 
                            ylabel="Anzahl eingehende\nAnrufe", legend=True, ax=axes[1])

    df_daily_calls.plot.bar(x="date", y=["Outbound_Verbunden_intern", "Outbound_Verloren_intern"], xlabel='Datum', 
                            ylabel="Anzahl ausgehende\nAnrufe", legend=True, ax=axes[2])

    df_daily_calls.plot.bar(x="date", y=["Outbound_Verbunden_extern", "Outbound_Verloren_extern"], xlabel='Datum', 
                            ylabel="Anzahl ausgehende\nAnrufe", legend=True, ax=axes[3])

    # Only show every 5th tick label
    for i, t in enumerate(axes[3].get_xticklabels()):
        if (i % 5) != 0:
            t.set_visible(False)

    # Set y-axis limit
    axes[0].set_ylim((0 , max([axes[0].get_ylim()[1], axes[1].get_ylim()[1], axes[2].get_ylim()[1], axes[3].get_ylim()[1]])))
    axes[1].set_ylim((0 , max([axes[0].get_ylim()[1], axes[1].get_ylim()[1], axes[2].get_ylim()[1], axes[3].get_ylim()[1]])))
    axes[2].set_ylim((0 , max([axes[0].get_ylim()[1], axes[1].get_ylim()[1], axes[2].get_ylim()[1], axes[3].get_ylim()[1]])))
    axes[3].set_ylim((0 , max([axes[0].get_ylim()[1], axes[1].get_ylim()[1], axes[2].get_ylim()[1], axes[3].get_ylim()[1]])))

    # Adjust the spacing at the bottom of the window
    plt.subplots_adjust(bottom=0.14, hspace=0.114)
    # Set main titel above all subplots
    if team == 'all':
        fig.suptitle("Auswertung der intern oder extern ein- und ausgehenden Anrufe während der Öffnungszeiten")
    else:
        fig.suptitle(f"Auswertung der intern oder extern ein- und ausgehenden Anrufe während der Öffnungszeiten ACHTUNG: Nur für Team: {team}")
    
    if save == False:
        # Show the plot
        plt.show()
    else:  
        # adjust the fig size for the saved image
        plt.gcf().set_size_inches(26, 10)
        # Save plot
        plt.savefig(f"Saved_Charts_Intern_Extern/daily_calls_team_{team}.png", bbox_inches='tight' , dpi=300)


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
    df_inbound = pd.read_sql_query(f"SELECT * FROM df_working_hours_inbound_extern", con)
    # close the connection to the database
    con.close()
    # Create a dict
    dict_duration = {
        "bis 6s": 0,
        "7 - 30s": 0,
        "30 - 60s": 0,
        "61 - 180s": 0,
        "ab 181s": 0,
    }
    # function to determin how many calls where how long
    def check_duration(row):
        if row['Rufdauer'] <= 6:
            dict_duration["bis 6s"] += 1
        elif row['Rufdauer'] <= 30:
            dict_duration["7 - 30s"] += 1
        elif row['Rufdauer'] <= 60:
            dict_duration["30 - 60s"] += 1
        elif row['Rufdauer'] <= 180:
            dict_duration["61 - 180s"] += 1
        else:
            dict_duration["ab 181s"] += 1
    # Call the function above on all rows
    df_inbound.apply(check_duration, axis='columns')
    # Creat a dataframe from a dict
    df = pd.DataFrame(dict_duration.items(), columns=['Rufdauer_Abstufung','Anzahl_Anrufe'])
    # Output the plot
    df.plot.bar(x="Rufdauer_Abstufung", y=["Anzahl_Anrufe"], xlabel='Rufdauer Abstufungen', 
                            ylabel="Anzahl Anrufe", legend=False)

    # Set titel
    plt.title("Auswertung der Rufdauer von eingehenden Anrufen von extern während der Öffnungszeiten")
    # Show the plot
    plt.show() 
