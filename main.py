import DBCURSORclass as dbc
import WRITEQUERYclass as wqc

import xml.etree.ElementTree as ET
from datetime import datetime as dt

def main():

    xml_file = 'config.xml'
    xml_root = ET.parse(xml_file).getroot()

    cur_time = None
    cur_sourse = None
    cur_db = None
    cur_schema = None
    cur_table = None
    cur_source_cred = None
    cur_target_cred_def = None
    cur_target_cred = None
    cur_source_cursor = None 
    cur_target_cursor_def = None 
    cur_target_cursor = None

    query_cr_db = None
    query_cr_schema = None
    query_info_table = None
    query_cr_table = None
    query_select_data = None
    query_insert_data = None
    query_insert_data_ex = None

    pack_size = 500
    pack_cur = 0
    pack_ex = 0
    sel_top = ' * '

    for sourse in xml_root :
        cur_sourse = sourse.attrib['name']
        print('Source name: ', cur_sourse)

        for db in sourse :
            cur_db = db.attrib["name"].lower()
            cur_time = str(dt.now().time())[0:13]
            print(f'{cur_time}: Create DB {cur_db}')
            cur_source_cred = f"{sourse.attrib['cred']}database={cur_db}"
            cur_target_cred = f"{sourse.attrib['target']}database="
            cur_target_cred_def = cur_target_cred + f'{sourse.attrib["db_target_default"]}'
            cur_source_cursor = dbc.DBCURSORclass(cur_source_cred, 'source')
            cur_target_cursor_def = dbc.DBCURSORclass(cur_target_cred_def, 'target')

            query_cr_db = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred_def)
            query_cr_db.w_query_cr_db(cur_db)
            cur_target_cursor_def.db_exec_q(query_cr_db.query)
            del cur_target_cursor_def

            cur_target_cred += cur_db
            cur_target_cursor = dbc.DBCURSORclass(cur_target_cred, 'target')

            for schema in db :
                cur_schema = schema.attrib['name']
                cur_time = str(dt.now().time())[0:13]
                print(f'{cur_time}: Create schema {cur_schema}')
                query_cr_schema = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred)
                query_cr_schema.w_query_cr_schema(cur_schema)
                cur_target_cursor.db_exec_q(query_cr_schema.query)

                for table in schema :
                    cur_table = table.attrib['name']
                    cur_time = str(dt.now().time())[0:13]
                    print(f'{cur_time}: Create and load table {cur_table}')

                    query_info_table = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred)
                    query_info_table.w_query_info_table(cur_db, cur_schema, cur_table)
                    cur_source_cursor.db_exec_q(query_info_table.query)

                    query_cr_table = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred)
                    query_cr_table.w_query_cr_table(cur_source_cursor.cursor, cur_db, cur_schema, cur_table)
                    cur_target_cursor.db_exec_q(query_cr_table.query)

                    query_select_data = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred)
                    query_select_data.w_query_select_data(cur_db, cur_schema, cur_table, sel_top)
                    cur_source_cursor.db_exec_q(query_select_data.query)

                    query_insert_data = wqc.WRITEQUERYclass(cur_source_cred, cur_target_cred)
                    query_insert_data.w_query_insert_data(cur_db, cur_schema, cur_table)
                    pack_cur = 0

                    for row_data in cur_source_cursor.cursor :
                        pack_cur += 1
                        #row_data = str(row_data).replace(u'\\xa0', ' ')
                        row_data_new = '('
                        for value_c in row_data :
                            if type(value_c) == type(1) :
                                row_data_new += f'{value_c},'
                            else :
                                row_data_new += f''' '{value_c}','''
                        row_data_new =  row_data_new[:-1] + ')'
                        if pack_ex == 1 or pack_cur == 1 :
                            query_insert_data_ex = query_insert_data.query + row_data_new
                            pack_ex = 0
                        else :
                            query_insert_data_ex  += row_data_new
                        if pack_cur % pack_size == 0 :
                            cur_target_cursor.db_exec_q(query_insert_data_ex)
                            pack_ex = 1
                            pack_cur=0
                            continue
                        query_insert_data_ex += ','
                    if pack_cur != 0 and pack_ex == 0 :
                        query_insert_data_ex = query_insert_data_ex[:-1]
                        cur_target_cursor.db_exec_q(query_insert_data_ex)
                        pack_ex=0
            del cur_source_cursor
            del cur_target_cursor

if __name__ == '__main__':
    main()