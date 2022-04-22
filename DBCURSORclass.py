import pyodbc
import sys
from pyodbc import Error
from datetime import datetime as dt

class DBCURSORclass :
    def __init__(self, cred, type):
        self.cred = cred
        self.type = type
        self.connection = None
        self.cursor = None
        self.cur_time = None
        self.db_connect()

    def db_connect(self) :
        self.cur_time = str(dt.now().time())[0:13]
        try:
            self.connection = pyodbc.connect(self.cred, autocommit=True)
            print(f'{self.cur_time}: Connection success to {self.type} DB: {self.cred}')
            self.cursor = self.connection.cursor()
        except (Exception, Error) as error:
            print(f"{self.cur_time}: Error connection to {self.type} !!! Error - {error}") 
            sys.exit()        

    def db_exec_q(self, query) :
        self.cur_time = str(dt.now().time())[0:13]
        try:
            
            self.cursor.execute(query)
        except (Exception, Error) as error:
            print(f"{self.cur_time}: Error exec query !!! Error - {error}") 
            sys.exit()

    def __del__(self):
        self.cur_time = str(dt.now().time())[0:13]
        self.cursor.close()
        self.connection.close()
        print(f'{self.cur_time}: Connection closed! {self.cred}')


    

