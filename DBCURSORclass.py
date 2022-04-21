import pyodbc
import sys
from pyodbc import Error

class DBCURSORclass :
    def __init__(self, cred, type):
        self.cred = cred
        self.type = type
        self.connection = None
        self.cursor = None
        self.db_connect()

    def db_connect(self) :
        try:
            self.connection = pyodbc.connect(self.cred, autocommit=True)
            print(f'Connection success to {self.type} DB: {self.cred}')
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            print(f"Error connection to {self.type} !!! Error - {error}") 
            sys.exit()        

    def db_exec_q(self, query) :
        try:
            self.cursor.execute(query)
        except (Exception, Error) as error:
            print(f"Error exec query !!! Error - {error}") 
            sys.exit()

    def __del__(self):
        self.cursor.close()
        self.connection.close()
        print(f'Connection closed! {self.cred}')


    

