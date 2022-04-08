import xml.etree.ElementTree as ET
import pyodbc
from pyodbc import Error

tree = ET.parse('config.xml')
root = tree.getroot()
db_cred=''
pg_prefix='am_'
source_t = '''SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
                FROM %(db_name)s.INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='%(s_name)s' and TABLE_NAME='%(t_name)s' '''
q_create_target_db= '''create database %(db_name)s'''
for sourse in root:
    print(sourse.tag, sourse.attrib)

    for db_name in sourse:

        if db_name.tag=='cred' :
            db_cred=db_name.text
        if db_name.tag=='target' :
            db_cred_target_all=db_name.text
            db_cred_target_first=db_cred_target_all + 'Database=' + db_name.attrib['db_default']
            print('\nTarget DB: ',db_cred_target_first)
            try:
                t_conn = pyodbc.connect(db_cred_target_first, autocommit=True)
                print("\nConnection success to target DB: ", db_name.attrib['db_default'],'\n')
            except (Exception, Error) as error:
                print("\nError connection! ", error)


        if db_name.tag=='DB' :
            print(db_name.tag, db_name.attrib)
            db_cred = db_cred + 'Database=' + db_name.attrib['name']
            print('\nSourse DB: ',db_cred)

            try:
                d_conn = pyodbc.connect(db_cred)
                print("Connection success to sourse DB: ", db_name.attrib['name'], '\n')
            except (Exception, Error) as error:
                print("\nError connection! ", error) 

            print('\nCreating DB in Target')
            cursor_targer = t_conn.cursor()
            target_db=(pg_prefix + db_name.attrib['name']).lower()
            q_create_db= 'CREATE DATABASE ' + target_db + ' OWNER for_py1;'
            print('\n',q_create_db)
            try:
                cursor_targer.execute(q_create_db)
            except (Exception, Error) as error:
                print("\nError connection! ", error)
            t_conn.close

            db_cred_target=db_cred_target_all + 'Database=' + target_db
            print(db_cred_target)
            try:
                t_conn = pyodbc.connect(db_cred_target, autocommit=True)
                cursor_targer = t_conn.cursor()
                print("\nConnection success to target DB: ", target_db,'\n')
            except (Exception, Error) as error:
                print("\nError connection! ", error)
           

            for s_name in db_name :
                print('\n',s_name.tag, s_name.attrib)
                print('\n','Creating schema')
                q_create_sc='CREATE SCHEMA ' + s_name.attrib['name']
                print(q_create_sc)
                cursor_targer.execute(q_create_sc)


                for t_name in s_name :
                    print(t_name.tag, t_name.attrib)
                    print('\nGetting inform sourse.')
                    t_name_param = {
                        'db_name' : db_name.attrib['name'] ,
                        's_name' : s_name.attrib['name'] ,
                        't_name' : t_name.attrib['name'] 
                    }
                    source_t_q = source_t % t_name_param
                    print(source_t_q)
                    cursor_t = d_conn.cursor()
                    cursor_t.execute(source_t_q)
                    for i in cursor_t:
                        print(i)
                    print('\n')

                    



            db_cred=''

