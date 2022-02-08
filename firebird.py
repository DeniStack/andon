import firebirdsql
import pandas as pd

from processing import tuple_to_df

class FirebirdConnection():

    def __init__(self, db_config: dict):
        self.connection = firebirdsql.connect(
            host=db_config['host'],
            database=db_config['database'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password']
        )
        self.cursor = self.connection.cursor()
    
    def close(self):
        self.connection.close()

    def execute_query(self, query_str: str) -> tuple:
        self.cursor.execute(query=query_str)
        records = self.cursor.fetchall()
        return records

    def query_notification_log(self, start_time: str=None) -> pd.DataFrame:
        without_param = """
            SELECT ID_MACH_STOP_HIST,
                TIMEFROM,
                ID_STOP_PROBLEM,
                DATEDIFF(SECOND, TIMEFROM, TIMETO)
            FROM MACH_STOP_HIST;
        """
        with_param = """
            SELECT ID_MACH_STOP_HIST,
                TIMEFROM,
                ID_STOP_PROBLEM,
                DATEDIFF(SECOND, TIMEFROM, TIMETO)
            FROM MACH_STOP_HIST
            WHERE TIMEFROM >= ?;
        """
        if start_time:
            self.cursor.execute(query=with_param, params=(start_time,))
        else:
            self.cursor.execute(query=without_param)
        records = self.cursor.fetchall()   
        records = tuple_to_df(tuple_list=records, columns=['ID', 'Timestamp', 'FailureCode', 'Duration'])
        return records

    def query_item_log(self, start_time: str = None) -> pd.DataFrame:
        without_param = """ SELECT ID_MACHINE, TIMECYCLE, ID_TYPE, ID_PROJECT
        FROM MACHINE_CYCLES;
        """
        with_param = """ SELECT ID_MACHINE, TIMECYCLE, ID_TYPE, ID_PROJECT
        FROM MACHINE_CYCLES
        WHERE TIMECYCLE >= ?;
        """
        if start_time:
            self.cursor.execute(query=with_param, params=(start_time,))
        else:
            self.cursor.execute(query=without_param)
        records = self.cursor.fetchall()   
        records = tuple_to_df(tuple_list=records, columns=['ID_machine', 'Timestamp', 'Result', 'ID_project'])
        return records

    def query_failure_description(self) -> pd.DataFrame:
        failure_description_query = """
        SELECT s1.ID_STOP_PROBLEM, s1.DESC, s2.DESC
            FROM STOP_PROBLEMS_DICT AS s1
        INNER JOIN STOP_PROBLEMS_DICT AS s2
        ON s1.ID_STOP_PROBLEM = s2.ID_STOP_PROBLEM
            WHERE s2.LANG = 'cs' AND s1.LANG = 'en'
            ;"""
        records = self.execute_query(failure_description_query)
        records = tuple_to_df(records, columns=['ID', 'FailureDescriptionDE', 'FailureDescriptionCZ'])
        return records
    
    def query_machines(self) -> pd.DataFrame:
        machine_query = """
        SELECT ID_MACHINE, DESC_EN
        FROM MACHINES;
        """
        records = self.execute_query(machine_query)
        records = tuple_to_df(records, columns=['ID', 'Description'])
        return records
    
    def query_projects(self) -> pd.DataFrame:
        projects_query = """
        SELECT ID_PROJECT, ID_MACHINE, NAME FROM PROJECTS;
        """
        records = self.execute_query(projects_query)
        records = tuple_to_df(records, columns=['ID_project', 'ID_machine', 'name'])
        return records