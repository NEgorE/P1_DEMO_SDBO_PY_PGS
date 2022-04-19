import DBCURSORclass as dbс
import xml.etree.ElementTree as ET

def main():

    xml_file = 'config.xml'
    xml_root = ET.parse(xml_file).getroot()

    cur_sourse = None
    cur_db = None
    cur_source_cred = None
    cur_target_cred_def = None
    cur_target_cred = None

    for sourse in xml_root :
        cur_sourse = sourse.attrib['name']
        print('Source name: ',cur_sourse)

        for db in sourse :
            if db.tag == 'cred' :
                cur_source_cred = db.text
            if db.tag=='target' :
                cur_target_cred_def = f'{db.text}Database={db.attrib["db_default"]}'
            if db.tag=='DB' :
                cur_source_cred += f'Database={db.attrib["name"]}'
                print(f'Connecting to source: {cur_source_cred} .....')
                #source_cursor = dbс.DBCURSORclass(cur_source_cred)



        
    


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
