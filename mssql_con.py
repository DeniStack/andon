from numpy import insert
import pandas as pd
import pyodbc

class MSSQLConnection:

    def __init__(self, db_config: dict):
        self.connection = pyodbc.connect(**db_config)
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()

    def execute_query(self, query_string: str, params: list = None):
        self.cursor.execute(query_string)
        records = self.cursor.fetchall()
        return records
    
    def insert_values(self, table_name: str, df: pd.DataFrame) -> None:
        column_str = '(' + ' '.join(' ' + column_name + ',' for column_name in df.columns)[1:-1] + ')'
        parameter_str = '(' + ' '.join(' ?,' for column_name in df.columns)[1:-1] + ')'
        for index, row in df.iterrows():
            #where_str = '\t\t\t'.join(' ' + column_name + ' = ' + str(row[column_name]) + ' AND \n' for column_name in df.columns)[0:-6]
            where_str = ''
            for column_name in df.columns:
                if isinstance(row[column_name], str):
                    value = "'{0}'".format(row[column_name])
                else:
                    value = str(row[column_name])
                if column_name == 'Timestamp':
                    where_str = where_str + "\t\t\t {0} = '{1}' AND\n".format(column_name, value)
                else:
                    where_str = where_str + "\t\t\t {0} = {1} AND\n".format(column_name, value)
            where_str = where_str[:-4]
            insert_str = '''
            BEGIN
                IF NOT EXISTS (SELECT * FROM {0} 
                    WHERE {3}
                    )
                BEGIN
                    INSERT INTO {0} {1}
                    VALUES {2}
                END
            END
            '''.format(table_name, column_str, parameter_str, where_str)
            self.cursor.execute(insert_str, *row)
        self.cursor.commit()
    
    def insert_values_without_check(self, table_name: str, df: pd.DataFrame) -> None:
        column_str = '(' + ' '.join(' ' + column_name + ',' for column_name in df.columns)[1:-1] + ')'
        parameter_str = '(' + ' '.join(' ?,' for column_name in df.columns)[1:-1] + ')'
        insert_str = '''
            BEGIN
                INSERT INTO {0} {1}
                VALUES {2}
            END
            '''.format(table_name, column_str, parameter_str)
        for index, row in df.iterrows():
            self.cursor.execute(insert_str, *row)
        self.cursor.commit()

   
