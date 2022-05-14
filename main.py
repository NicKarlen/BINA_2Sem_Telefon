import prepare
import analyse
import plot_data
import pandas as pd
import os

# Insert the DB name which is stored in the forlder Analyse/DB
db_name = "db_anon.db"
# Find the path where this script is located
base_directory = os.path.dirname(os.path.realpath(__file__))
# DB Path
db_path = f"{base_directory}/{db_name}"


""" Main """
if __name__ == "__main__":
    print("Code running..........")

    """ Gather Data """
    # prepare.prep() # takes about 15min!!

    """ Build Data Models """
    # analyse.filter_workinghours(db_path)
    # analyse.amount_of_calls_during_hours(db_path, 'inbound')
    # analyse.amount_of_calls_during_hours(db_path, 'outbound')
    # analyse.amount_of_calls_during_weekdays(db_path, 'inbound')
    # analyse.amount_of_calls_during_weekdays(db_path, 'outbound')
    # analyse.amount_of_calls_during_months(db_path, 'inbound')
    # analyse.amount_of_calls_during_months(db_path, 'outbound')
    # analyse.amount_of_calls_each_date(db_path, 'inbound')
    analyse.amount_of_calls_each_date(db_path, 'outbound')
    # analyse.amount_of_calls_from_same_number(db_path)

    """ Explore the Data """
    # plot_data.plot_amount_of_calls_during_X(db_path)
    # plot_data.plot_amount_of_daily_calls(db_path)
    # plot_data.plot_amount_of_calls_from_same_number(db_path)


    print("Code finished.........")