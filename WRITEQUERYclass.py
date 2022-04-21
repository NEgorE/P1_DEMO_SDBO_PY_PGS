class WRITEQUERYclass :
    def __init__(self):
        self.cur_db = None
        self.cur_schema = None
        self.cur_table = None

        self.query = None
        self.quey_type = None

        self.dt_dict = {
            'int' : 'integer',
            'nvarchar' : 'character varying',
            'date' : 'date',
            'smallint' : 'smallint',
            'tinyint' : 'smallint',
            'datetime' : 'time without time zone',
            'time' : 'time without time zone'
        }

    def w_query_cr_db(self, cur_db) :
        self.cur_db = cur_db
        self.query = f'CREATE DATABASE {self.cur_db};'

    def w_query_cr_schema(self, cur_schema) :
        self.cur_schema = cur_schema
        self.query = f'CREATE SCHEMA {self.cur_schema};'
    
    def w_query_info_table(self, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'''SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                FROM {self.cur_db}.INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='{self.cur_schema}' and TABLE_NAME='{self.cur_table}' '''
    
    def w_query_cr_table(self, cursor, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'CREATE TABLE {self.cur_db}.{self.cur_schema}.{self.cur_table} ('
        x = 1
        for i in cursor:
            f_len = ''
            if x > 1 :
                self.query += ','
            self.query += f'\n{i.COLUMN_NAME} {self.dt_dict[i.DATA_TYPE]}'
            if i.CHARACTER_MAXIMUM_LENGTH != None :
                f_len += f'({str(i.CHARACTER_MAXIMUM_LENGTH)})' 
            self.query += f_len
            x += 1
        self.query += '\n);'
    
    def w_query_select_data(self, cur_db, cur_schema, cur_table, sel_top) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        if sel_top == -1 :
            self.query = f'Select * from {self.cur_db}.{self.cur_schema}.{self.cur_table};'
        else :
            self.query = f'Select top {sel_top} * from {self.cur_db}.{self.cur_schema}.{self.cur_table};'

    def w_query_insert_data(self, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'Insert into {self.cur_db}.{self.cur_schema}.{self.cur_table} values'


