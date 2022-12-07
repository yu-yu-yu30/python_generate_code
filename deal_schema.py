#step1:read from schema and save to a picke file

import re
import pickle
def write_pickle():
    rr = re.compile(r'class\s*(\w+)\{(.*?)\}')
    final_struct = []
    f = open('testcl/schema.txt', 'r', encoding='utf-8')
    x = f.read()
    x = re.sub('\n', '', x)
    x = re.sub('\s+', ' ', x)
    c = rr.findall(x)
    for cc in c:
        temp_dict = {}
        temp_dict['tablename'] = cc[0]
        temp_keys = {}
        temp_index = []
        temp_pks = set()
        temp_pindex = []
        for yii in cc[1].split(';'):
            if yii == '':
                continue
            yi = yii.lstrip()
            try:
                value, key = yi.rsplit(' ', 1)
            except:
                print (yii)
            if 'tree' not in value:
                temp_keys[key] = value
            else:
                temp_pkss = re.search(r'(\w*tree)\s*<(.*?)>', value).group(2)
                # index = key.rsplit('_',1)
                temp_x = temp_pkss.split(',')
                temp_index.append((temp_x, key))
                temp_pks = temp_pks.union(set(temp_x))
                if 'unique' in value:
                    temp_pindex = [temp_x,key]


        temp_dict['keydict'] = temp_keys
        temp_dict['index'] = temp_index
        temp_dict['pk'] = list(temp_pks)
        temp_dict['pindex'] = temp_pindex
        final_struct.append(temp_dict)

    with open('data/table.pickle', 'wb') as f:
        pickle.dump(final_struct, f)

