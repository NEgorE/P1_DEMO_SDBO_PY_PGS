import xml.etree.ElementTree as ET
import pyodbc
from pyodbc import Error


dt_dict = {
    'int' : 'integer',
    'nvarchar' : 'character',
    'date' : 'date',
    'smallint' : 'smallint',
    'tinyint' : 'smallint',
    'datetime' : 'timestamp without time zone',
    'time' : 'timestamp without time zone'
}


tree = ET.parse('config.xml')
root = tree.getroot()
db_cred=''
source_t = '''SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                FROM %(db_name)s.INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='%(s_name)s' and TABLE_NAME='%(t_name)s' '''
source_t_r = '''SELECT COUNT(*)
                FROM %(db_name)s.INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='%(s_name)s' and TABLE_NAME='%(t_name)s' '''
q_create_target_db= '''create database %(db_name)s'''
for sourse in root:
    #print(sourse.tag, sourse.attrib)

    for db_name in sourse:

        if db_name.tag=='cred' :
            db_cred=db_name.text
        if db_name.tag=='target' :
            db_cred_target_all=db_name.text
            db_cred_target_first=db_cred_target_all + 'Database=' + db_name.attrib['db_default']
            #print('Target DB: ',db_cred_target_first)
            try:
                t_conn = pyodbc.connect(db_cred_target_first, autocommit=True)
                cursor_targer = t_conn.cursor()
                cursor_targer.execute('select 1')
                print("Connection success to target DB: ", db_name.attrib['db_default'],'')
            except (Exception, Error) as error:
                print("Error connection! ", error)

        if db_name.tag=='DB' :
            #print(db_name.tag, db_name.attrib)
            #db_cred = db_cred + 'Database=' + db_name.attrib['name']
            #print('Sourse DB: ',db_cred)

            try:
                d_conn = pyodbc.connect(db_cred)
                print("Connection success to sourse DB: ", db_name.attrib['name'], '\n')
            except (Exception, Error) as error:
                print("Error connection!! ", error) 

            #print('Creating DB in Target')
            cursor_targer = t_conn.cursor()
            target_db=(db_name.attrib['name']).lower()
            q_create_db= 'CREATE DATABASE ' + target_db + ' OWNER for_py1;'
            #print('',q_create_db)
            cursor_targer.execute(q_create_db)

            db_cred_target=db_cred_target_all + 'Database=' + target_db
            #print(db_cred_target)
            t_conn2 = pyodbc.connect(db_cred_target, autocommit=True)
            cursor_targer2 = t_conn2.cursor()
            cursor_targer2.execute('select 1')

            for s_name in db_name :
                #print('\n',s_name.tag, s_name.attrib)
                #print('\n','Creating schema')
                q_create_sc='CREATE SCHEMA ' + s_name.attrib['name'] + ' AUTHORIZATION for_py1;'
                #print(q_create_sc)
                cursor_targer2.execute('select 1')
                cursor_targer2.execute(q_create_sc)
                for t_name in s_name :
                    print(t_name.tag, t_name.attrib)
                    print('Getting inform sourse.\n')
                    t_name_param = {
                        'db_name' : db_name.attrib['name'] ,
                        's_name' : s_name.attrib['name'] ,
                        't_name' : t_name.attrib['name'] 
                    }

                    source_t_q = source_t % t_name_param
                    #print(source_t_q)

                    cursor_t_r= d_conn.cursor()
                    source_t_rс = source_t_r % t_name_param
                    cursor_t_r.execute(source_t_rс)
                    nof = cursor_t_r.fetchone()[0]

                    cursor_t = d_conn.cursor()
                    cursor_t.execute(source_t_q)
                    

                    create_table_q='CREATE TABLE ' + db_name.attrib['name'] + '.' + s_name.attrib['name'] + '.' + t_name.attrib['name'] + ' ('
                    cnof=0
                    for i in cursor_t:
                        cnof=cnof+1
                        print(i)
                        print(i.COLUMN_NAME)
                        print(i.DATA_TYPE,' ',dt_dict[i.DATA_TYPE])
                        print(i.CHARACTER_MAXIMUM_LENGTH)
                        create_table_q = create_table_q + '\n' + i.COLUMN_NAME + ' ' + dt_dict[i.DATA_TYPE]
                        f_len=''
                        if i.CHARACTER_MAXIMUM_LENGTH != None :
                            f_len = f_len + '('+ str(i.CHARACTER_MAXIMUM_LENGTH) + ')'
                        if cnof<nof:
                            f_len = f_len + ','
                        
                        create_table_q = create_table_q + f_len
                    create_table_q = create_table_q + '\n);'
                    
                    print(create_table_q)
                    cursor_targer2.execute(create_table_q)

            db_cred=''

