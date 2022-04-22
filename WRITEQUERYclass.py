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
        self.query = f'''SELECT c.COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT, CONSTRAINT_TYPE
                FROM {self.cur_db}.INFORMATION_SCHEMA.COLUMNS c
                left join {self.cur_db}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on ccu.TABLE_SCHEMA = c.TABLE_SCHEMA and ccu.TABLE_NAME=c.TABLE_NAME and ccu.COLUMN_NAME=c.COLUMN_NAME
                left join {self.cur_db}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on tc.CONSTRAINT_NAME=ccu.CONSTRAINT_NAME
                where c.TABLE_SCHEMA='{self.cur_schema}' and c.TABLE_NAME='{self.cur_table}' '''
    
    def w_query_cr_table(self, cursor, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'CREATE TABLE {self.cur_db}.{self.cur_schema}.{self.cur_table} ('
        x = 1
        for i in cursor:
            c_name = i.COLUMN_NAME
            c_d_type = self.dt_dict[i.DATA_TYPE]
            c_lenght = i.CHARACTER_MAXIMUM_LENGTH
            c_nullable = i.IS_NULLABLE
            c_default = i.COLUMN_DEFAULT
            c_pkey = i.CONSTRAINT_TYPE
            f_len = ''
            self.query += f'\n{c_name} {c_d_type} '
            if c_lenght != None :
                f_len += f'({str(c_lenght)}) ' 
            if c_nullable == 'NO' :
                f_len += 'NOT NULL '
            else :
                f_len += 'NULL '
            if c_default != None :
                f_len += f'default {c_default[1:-1]} '
            if c_pkey != None :
                f_len += f'{c_pkey} '
            self.query += f'{f_len},'
        self.query = self.query[:-1] + '\n);'
    
    def w_query_select_data(self, cur_db, cur_schema, cur_table, sel_top) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'Select {sel_top} from {self.cur_db}.{self.cur_schema}.{self.cur_table};'

    def w_query_insert_data(self, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'Insert into {self.cur_db}.{self.cur_schema}.{self.cur_table} values'


