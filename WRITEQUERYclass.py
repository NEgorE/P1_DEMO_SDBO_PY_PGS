class WRITEQUERYclass :
    def __init__(self, cred_s, cred_t):
        self.cur_db = None
        self.cur_schema = None
        self.cur_table = None

        self.query = None
        self.quey_type = None

        self.type_db_s = None
        self.type_db_t = None

        self.dt_dict = {
            'int' : 'integer',
            'nvarchar' : 'character varying',
            'date' : 'date',
            'smallint' : 'smallint',
            'tinyint' : 'smallint',
            'datetime' : 'time without time zone',
            'time' : 'time without time zone'
        }
        self.find_db_type(cred_s, 'sourse')
        self.find_db_type(cred_t, 'target')

    def find_db_type(self, cred, type_c):
        d = 'DRIVER='
        x = cred.find(d,1) + len(d) + 1
        y = cred.find(';',1)
        type_db_1 = cred[x:y]
        if type_db_1.upper().count('POSTGRESQL') > 0 :
            type_db_2 = 'PSQL'
        elif type_db_1.upper().count('SQL SERVER') > 0 :
            type_db_2 = 'MSSQL'
        if type_c == 'sourse' :
            self.type_db_s = type_db_2
        elif type_c == 'target' :
            self.type_db_t = type_db_2

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
                where c.TABLE_SCHEMA='{self.cur_schema}' and c.TABLE_NAME=lower('{self.cur_table}') '''
    
    def w_query_cr_table(self, cursor, cur_db, cur_schema, cur_table) :
        self.cur_db = cur_db
        self.cur_schema = cur_schema
        self.cur_table = cur_table
        self.query = f'CREATE TABLE {self.cur_db}.{self.cur_schema}.{self.cur_table} ('
        x = 1
        for i in cursor:
            c_name = i.column_name
            if self.type_db_s == self.type_db_t :
                c_d_type = i.data_type
            else :
                c_d_type = self.dt_dict[i.data_type]
            c_lenght = i.character_maximum_length
            c_nullable = i.is_nullable
            c_default = i.column_default
            c_pkey = i.constraint_type
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


