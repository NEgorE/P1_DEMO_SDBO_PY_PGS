import DBCURSORclass as dbс
import WRITEQUERYclass as wqc
import xml.etree.ElementTree as ET

def main():

    xml_file = 'config.xml'
    xml_root = ET.parse(xml_file).getroot()

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

    for sourse in xml_root :
        cur_sourse = sourse.attrib['name']
        print('Source name: ',cur_sourse)

        for db in sourse :
            if db.tag == 'cred' :
                cur_source_cred = db.text
            if db.tag=='target' :
                cur_target_cred = f'{db.text}Database='
                cur_target_cred_def = cur_target_cred + db.attrib["db_default"]
            if db.tag=='DB' :
                cur_db = db.attrib["name"].lower()
                cur_source_cred += f'Database={cur_db}'
                cur_source_cursor = dbс.DBCURSORclass(cur_source_cred,'source')
                cur_target_cursor_def = dbс.DBCURSORclass(cur_target_cred_def,'target')

                query_cr_db = wqc.WRITEQUERYclass()
                query_cr_db.w_query_cr_db(cur_db)
                cur_target_cursor_def.db_exec_q(query_cr_db.query)
                del cur_target_cursor_def

                cur_target_cred += cur_db
                cur_target_cursor = dbс.DBCURSORclass(cur_target_cred,'target')

                for schema in db :
                    cur_schema = schema.attrib['name']
                    query_cr_schema = wqc.WRITEQUERYclass()
                    query_cr_schema.w_query_cr_schema(cur_schema)
                    cur_target_cursor.db_exec_q(query_cr_schema.query)

                    for table in schema :
                        cur_table = table.attrib['name']
                        print(f'Create and load table {cur_table}')

                        query_info_table = wqc.WRITEQUERYclass()
                        query_info_table.w_query_info_table(cur_db, cur_schema, cur_table)
                        cur_source_cursor.db_exec_q(query_info_table.query)

                        query_cr_table = wqc.WRITEQUERYclass()
                        query_cr_table.w_query_cr_table(cur_source_cursor.cursor, cur_db, cur_schema, cur_table)
                        cur_target_cursor.db_exec_q(query_cr_table.query)

                        query_select_data = wqc.WRITEQUERYclass()
                        query_select_data.w_query_select_data(cur_db, cur_schema, cur_table)
                        cur_source_cursor.db_exec_q(query_select_data.query)

                        query_insert_data = wqc.WRITEQUERYclass()
                        query_insert_data.w_query_insert_data(cur_db, cur_schema, cur_table)
                        pack_cur = 0
                        for row_data in cur_source_cursor.cursor :
                            pack_cur+=1
                            row_data = str(row_data).replace(u'\\xa0', ' ')
                            if pack_ex == 1 or pack_cur == 1 :
                                query_insert_data_ex = query_insert_data.query + row_data
                                pack_ex = 0
                            else :
                                query_insert_data_ex  = query_insert_data_ex + row_data
                            if pack_cur % pack_size == 0 :
                                cur_target_cursor.db_exec_q(query_insert_data_ex)
                                pack_ex = 1
                                pack_cur=0
                                continue
                            query_insert_data_ex = query_insert_data_ex + ','
                        if pack_cur != 0 and pack_ex == 0 :
                            query_insert_data_ex = query_insert_data_ex[:-1]
                            cur_target_cursor.db_exec_q(query_insert_data_ex)
                            pack_ex=0

if __name__ == '__main__':
    main()
