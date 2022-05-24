import prepare
import analyse
import plot_data
import pandas as pd
import os
from datetime import datetime

# Insert the DB name which is stored in the forlder Analyse/DB
db_name = "db_anon.db"
# Find the path where this script is located
base_directory = os.path.dirname(os.path.realpath(__file__))
# DB Path
db_path = f"{base_directory}/{db_name}" 


""" Main """
if __name__ == "__main__":
    print("Code running..........", datetime.now())

    """ Gather Data """
    # prepare.prep() # takes about 15min!!

    """ Build Data Models """
    # analyse.filter_workinghours(db_path)
    analyse.run_complete_analysis_for_all_Teams(db_path) # takes about 15min!!

    # analyse.amount_of_calls_from_same_number(db_path)

    """ Explore the Data """
    # plot_data.plot_amount_of_calls_during_X(db_path, 'all') # second parameter is the team, if all teams insert 'all'
    # plot_data.plot_amount_of_daily_calls(db_path)
    # plot_data.plot_amount_of_calls_from_same_number(db_path)
    # plot_data.plot_call_duration(db_path) # --> data makes no sense...

    print("Code finished.........", datetime.now())