import firebirdsql
import pandas as pd

from datetime import datetime, timedelta
from firebird import FirebirdConnection
from fb_config import FB_ANDON_CONNECTION
from mssql_con import MSSQLConnection
from mssql_config import MSSQL_ANDON_CONNECTION
from numpy import int64
from processing import set_query_date, tuple_to_df, convert_result_code

def main():
    current_date = datetime.today()
    fb_conn = FirebirdConnection(db_config=FB_ANDON_CONNECTION)
    ms_sql_conn = MSSQLConnection(db_config=MSSQL_ANDON_CONNECTION)
    # Update the static tables on the first day of the week at midnight
    if current_date.weekday() == 0 and current_date.hour == 0:
        # Fetch the FailureDescription Entries
        failure_description = fb_conn.failure_description_query()
        # Fetch the machines table
        machines = fb_conn.query_machines()
        # Fetch the project table
        projects = fb_conn.query_projects()
        # Insert into MSSQL Database
        ms_sql_conn.insert_values('FailureDescription', failure_description)
        ms_sql_conn.insert_values('Machines', machines)
        ms_sql_conn.insert_values('Projects', projects)
    # Query date is the date at execution time minus one hour to the full hour
    query_date = set_query_date(current_date)
    query_date_str = query_date.strftime('%Y-%m-%d %H:%M:%S')
    # Fetch the entries from 'MACHINE_STOP_HIST' and remap them to 'NotificationLog'
    notification_log = fb_conn.query_notification_log(start_time=query_date_str)
    # Fetch the entries from 'MACHINE_CYCLES' and remap them to 'ItemLog'
    item_log = fb_conn.query_item_log(start_time=query_date_str)
    # Insert into MSSQL database
    notification_log = notification_log.fillna(0)
    ms_sql_conn.insert_values('NotificationLog', notification_log)
    ms_sql_conn.insert_values('ItemLog', item_log)
    fb_conn.close()
    ms_sql_conn.close()

def dump_dbs_to_csv():
    fb_conn = FirebirdConnection(db_config=FB_ANDON_CONNECTION)
    machines = fb_conn.query_machines()
    machines.to_csv('Machines.csv', index=False)
    projects = fb_conn.query_projects()
    projects.to_csv('Projects.csv', index=False)
    failure_description = fb_conn.query_failure_description()
    failure_description.to_csv('FailureDescription.csv', index=False)
    item_log = fb_conn.query_item_log()
    # Convert 'Result' column
    item_log['Result'] = item_log['Result'].apply(convert_result_code)
    item_log.to_csv('ItemLog.csv', index=False)
    notification_log = fb_conn.query_notification_log()
    # Fill 'None' and convert to Integer
    notification_log = notification_log.fillna(0)
    notification_log['FailureCode'] = notification_log['FailureCode'].astype(int64)
    notification_log['Duration'] = notification_log['Duration'].astype(int64)
    notification_log['Type'] = 0
    notification_log['DefectCode'] = 0
    notification_log.to_csv('NotificationLog.csv', index=False)
    fb_conn.close()

# def main():
#    dump_dbs_to_csv()

if __name__ == '__main__':
    main()
