import  load
from deal_schema import write_pickle
from table_db import *
def read_db_manager():
    with open('./testcl/dbmanager.hpp','r',encoding ='utf8') as f:
        x = f.readlines()
    return x

def write_db_manager(table_names):
    lines = read_db_manager()
    for i,line in enumerate(lines):
        if '#include \"tablebase.h\"' in line:
            for name in table_names:
                #这样可能可以不重复吧
                if f'#include \"{name.lower()}.h\"\n' in lines:
                    continue
                lines.insert(i+1,f'#include \"{name.lower()}.h\"\n')
        if 'if (!m_database.Create(\".\", "tradedb", 1000000)) return false;' in line:
            for name in table_names:
                if f'        create_table(\"{name}\", std::make_shared<Table{name}>(m_database.GetDBHandle()));\n' in lines:
                    continue
                lines.insert(i+1,f'        create_table(\"{name}\", std::make_shared<Table{name}>(m_database.GetDBHandle()));\n')
    with open('./testcl/dbmanager.hpp','w') as f:
        f.write(''.join(lines))



if __name__ == '__main__':
    write_pickle()
    data = load.load_from_pickle()
    #print (data)
    for dd in data[11:12]:
        db = Db('./testc','./testh','./testcl',**dd)
        x= list(db.get_permutation_and_combine())
        db.write_file()
    #input is list of the tablenames,and the initial file dbmanager.hpp should not include any of tables
    write_db_manager([dd['tablename'] for dd in data])

