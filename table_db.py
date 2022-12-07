import os
class Db:
    def __init__(self,*args,**kwargs):
        #assert len(args)>=3
        self.i_size = 5
        # self.mapp = {'string':'PyStr_To_CString','int8':'PyLong_AsLong','double':'PyFloat_AsDouble'}
        self.tablename = kwargs['tablename']
        self.keydict = kwargs['keydict']
        self.pks = kwargs['pk']
        self.index = kwargs['index']
        self.pindex = kwargs['pindex']
        self.judge_dir(args[0])
        self.judge_dir(args[1])
        self.judge_dir(args[2])
        self.cfile_path = args[0]+'/'+self.tablename.lower()+'.cpp'
        self.hfile_path = args[1]+'/'+self.tablename.lower()+'.h'
        self.clfile_path = args[2]+'/'+'schema.txt'
        self.pks = sorted(self.pks, key=lambda x: list(self.keydict.keys()).index(x))

    @staticmethod
    def get_all_permutation(nums):
        final_list = []
        temp = []

        def dfs(nums, tar):
            if tar == len(nums):
                final_list.append(temp[:])
                return
            temp.append(f'{nums[tar]}==keyDict.cend()')
            dfs(nums, tar + 1)
            temp.pop()
            temp.append(f'{nums[tar]}!=keyDict.cend()')
            dfs(nums, tar + 1)
            temp.pop()

        dfs(nums, 0)
        return final_list

    def get_all_cstringpermute(self, nums):
        final_list = []
        temp = []

        def dfs(nums, tar):
            if tar == len(nums):
                final_list.append(temp[:])
                return
            dfs(nums, tar + 1)
            temp.append(f'{self.get_mapped(nums[tar][3:])}(' + f'{nums[tar]}' + '->second.ptr())')
            dfs(nums, tar + 1)
            temp.pop()

        dfs(nums, 0)
        return final_list

    @staticmethod
    def get_all_permutate(nums):
        final_list = []
        temp = []

        def dfs(nums, tar):
            if tar == len(nums):
                final_list.append(temp[:])
                return
            dfs(nums, tar + 1)
            temp.append(f'{nums[tar]}')
            dfs(nums, tar + 1)
            temp.pop()

        dfs(nums, 0)
        ffinal_list = []
        for final in final_list:
            if final == []:
                ffinal_list.append('false')
            else:
                ffinal_list.append('table.find_by_' + '_'.join(final) + '_INDEX')
        return ffinal_list

    @staticmethod
    def get_all_cls(nums):
        final_list = []
        temp = []

        def dfs(nums, tar):
            if tar == len(nums):
                if len(nums) == len(temp):
                    return
                final_list.append(temp[:])
                return
            dfs(nums, tar + 1)
            temp.append(f'{nums[tar]}')
            dfs(nums, tar + 1)
            temp.pop()

        dfs(nums, 0)
        return final_list


    @ staticmethod
    def judge_dir(path):
        if not os.path.isdir(path):
            os.makedirs(path)


    def get_mapped(self,key):
        #key值必须存在
        k_prop = self.keydict[key]
        if 'string' in k_prop:
            return 'PyStr_To_CString'
        if 'double' in k_prop:
            return 'py::cast<double>'
        if 'int' in k_prop:
            return 'py::cast<long>'

    def get_ver_mapped(self,key):
        #key值必须存在

        k_prop = self.keydict[key]
        if 'string' in k_prop:
            return 'PyUnicode_FromString'
        if 'double' in k_prop:
            return 'PyFloat_FromDouble'
        if 'int' in k_prop:
            return 'PyLong_FromLong'


    def gen_find_key(self,pks,equal = ''):
        find_list = []
        for pk in pks:
            find_list.append(f'{equal}keyDict.contains(\"{pk.upper()}\")')
        return find_list

    def gen_cend(self,pks):
        pk_out = list(set(self.pks).difference(set(pks)))
        pks = ['ite'+pk for pk in pks]
        pk_out = ['ite'+pk for pk in pk_out]
        list_p = []
        for pk in pks:
            list_p.append(pk)
        for pk in pk_out:
            list_p.append("!"+pk)
        return list_p

    def gen_find(self,index):
        return 'table.find_by_'+index

    def gen_cs(self,pks):
        list_p = []
        for pk in pks:
            list_p.append(f'{self.get_mapped(pk)}(keyDict[\"{pk.upper()}\"])')
        return list_p

    def get_permutation_and_combine(self):
        list_p = []
        for pks,index in self.index:
            list_p.append((self.gen_cend(pks),self.gen_find(index),self.gen_cs(pks)))
        return list_p


    def gen_include(self):
        yield f'#include \"{self.tablename.lower()}.h\"\n'
        yield '\n'
        yield '\n'

    def gen_insert(self):
        #局部indent看下能不能做个缩进
        indent = 0
        temp_keys = self.pks
        yield f'TableBase::objecttype Table{self.tablename}::insert_data(const TableBase::dicttype& valueDict)\n'
        yield '{\n'
        indent+=1
        yield ' '*indent*self.i_size+'if (!primary_constriant(valueDict)) { return TABLE_ERROR_MSG(tableError::MISSING_PRIMRARY_KEY); }\n'
        yield '\n'
        yield ' '*indent*self.i_size+'table.newrecord();\n'
        yield '\n'
        yield ' '*indent*self.i_size+'if (!set_data(valueDict)) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }\n'
        indent+=1
        yield ' '*indent*self.i_size+'if (!table.append(\n '
        for i,pk in enumerate(temp_keys):
            if i <len(temp_keys)-1:
                yield ' '*indent*self.i_size+f'{self.get_mapped(pk)}(valueDict[\"{pk}\"]),\n'
            else:
                yield ' '*indent*self.i_size+f'{self.get_mapped(pk)}(valueDict[\"{pk}\"])\n'
        indent-=1
        yield ' '*indent*self.i_size+')) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }\n'
        #yield ' '*indent*self.i_size+'{ return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }\n'
        yield ' '*indent*self.i_size+'return py::none();\n'
        indent -=1
        yield indent*self.i_size*' '+'}\n'
        yield'\n'
        yield '\n'


    def gen_move_cursor(self):
        pk_list = self.get_permutation_and_combine()
        indent = 0
        yield f'bool Table{self.tablename}::move_cursor(const TableBase::dicttype& keyDict)\n'
        yield '{\n'
        indent+=1
        for pk in self.pks:
            yield indent*self.i_size*' '+ f'auto ite{pk} = keyDict.contains(\"{pk}\");\n'
        for icend,ifind,ics in pk_list:
            yield indent*' '*self.i_size+'if ('+'&&'.join(icend)+')\n'
            yield indent*' '*self.i_size+'{\n'
            indent+=1
            if ifind=='false':
                yield indent*' '*self.i_size+ 'return false;\n'
                indent-=1
                yield indent*' '*self.i_size+'}\n'
                continue
            else:
                yield indent*' '*self.i_size+ f'return {ifind}(\n'
                indent+=1
                for i,cs in enumerate(ics):
                    if i!=len(ics)-1:
                        yield indent*' '*self.i_size+cs+',\n'
                    else:
                        yield indent*' '*self.i_size+cs+'\n'
                indent-=1
                yield indent*' '*self.i_size+');\n'
                indent-=1
                yield indent*' '*self.i_size+'}\n'
                continue
        yield indent*' '*self.i_size+' return false;\n'
        indent-=1
        yield indent*' '*self.i_size+'}\n'
        yield '\n'
        yield '\n'



    def gen_set(self):
        indent = 0
        yield f'bool Table{self.tablename}::set_data(const TableBase::dicttype& valueDict,bool overwrite)\n'
        yield '{\n'
        indent+=1
        yield ' '*self.i_size*indent+'auto success = true;\n'
        #注意这里opera初始值为true
        yield ' '*self.i_size*indent+'auto oper = true;\n'
        pks = list(set(self.keydict.keys()).difference(set(self.pks)))
        for pk in pks:
            yield ' '*self.i_size*indent+f'if (valueDict.contains(\"{pk.upper()}\"))\n '
            yield ' '*self.i_size*indent+'{\n'
            indent+=1
            temp_str = self.get_mapped(pk)
            if temp_str=='PyStr_To_CString':
                yield ' '*self.i_size*indent+f'oper = table.set_{pk.upper()}(const_cast<char *>({temp_str}(valueDict[\"{pk.upper()}\"])));\n'
            else:
                yield ' ' * self.i_size * indent + f'oper = table.set_{pk.upper()}({temp_str}(valueDict[\"{pk.upper()}\"]));\n'

            indent-=1
            yield ' '*self.i_size*indent+'}\n'
            yield ' '*self.i_size*indent+'else\n'
            yield ' '*self.i_size*indent+'{\n'
            indent+=1
            if  temp_str=='PyStr_To_CString':
                yield ' '*self.i_size*indent+'if (overwrite) '+f'oper = table.set_{pk.upper()}(const_cast<char *>(""));\n'
            elif temp_str=='py::cast<double>':
                yield ' '*self.i_size*indent+'if (overwrite) '+f'oper = table.set_{pk.upper()}(0.0);\n'
            elif temp_str=='py::cast<long>':
                yield ' ' * self.i_size * indent +'if (overwrite) '+ f'oper = table.set_{pk.upper()}(0);\n'
            else:
                raise Exception("没找到没找到没找到类型.....")

            indent-=1
            yield ' '*self.i_size*indent+'}\n'
            yield ' '*self.i_size*indent+'success = success && oper;\n'
            yield '\n'
        yield ' '*self.i_size*indent+'return success;\n'
        indent-=1
        yield '}\n'
        yield '\n'
        yield '\n'


    def gen_update(self):
        indent = 0
        yield f'TableBase::objecttype Table{self.tablename}::update_data(const TableBase::dicttype& keyDict, const TableBase::dicttype& valueDict)\n'
        yield '{\n'
        indent+=1
        yield indent*self.i_size*' '+ f'if ({"||".join(self.gen_find_key(self.pindex[0],"!"))})\n'
        yield indent*self.i_size*' '+'{\n'
        indent+=1
        yield indent*self.i_size*' '+ 'return TABLE_ERROR_MSG(tableError::MISSING_PRIMRARY_KEY);\n'
        indent-=1
        yield indent*self.i_size*' '+'}\n'
        yield indent*self.i_size*' '+f'if (!table.find_by_{self.pindex[1]}(\n'
        indent+=1
        for i,pk in enumerate(self.pindex[0]):
            if i==len(self.pindex[0])-1:
                yield indent*self.i_size*' '+f'{self.get_mapped(pk)}(keyDict[\"{pk}\"])\n'
            else:
                yield indent*self.i_size*' '+f'{self.get_mapped(pk)}(keyDict[\"{pk}\"]),\n'
        indent-=1
        yield indent*self.i_size*' '+'))  { return  TABLE_ERROR_MSG(tableError::RECORD_NOT_FOUND); }\n'
        yield indent*self.i_size*' '+'if (!set_data(valueDict, false)) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }\n'
        yield indent*self.i_size*' '+'return py::none();\n'
        indent-=1
        yield '}\n'
        yield '\n'
        yield '\n'
        yield '\n'


    def gen_primary_constraints(self):
        indent = 0
        yield f'bool Table{self.tablename}::primary_constriant(const dicttype& keyDict)\n'
        yield '{\n'
        indent+=1
        yield indent*self.i_size*' '+ f'return ({"&&".join(self.gen_find_key(self.pindex[0]))});\n'
        indent-=1
        yield '}\n'
        yield '\n'
        yield '\n'

    def gen_gen_dict(self):
        indent = 0
        yield f'TableBase::dicttype Table{self.tablename}::gen_dict()\n'
        yield '{\n'
        indent+=1
        yield indent*self.i_size*' '+'dicttype d;\n'
        for i,pk in enumerate(self.keydict.keys()):
            yield indent * self.i_size * ' ' + f'd[\"{pk.upper()}\"] = py::cast(table.get_{pk.upper()}());\n'
        yield indent * self.i_size * ' ' +'return d;\n'
        indent-=1
        yield indent*self.i_size*' '+'}\n'
        yield '\n'
        yield '\n'
        yield '\n'

    #手动抄写
    def gen_select_items(self):
        yield f'TableBase::listdicttype Table{self.tablename}::select_items(const TableBase::dicttype& keyDict)\n'
        yield '{\n' \
              '     py::list l;\n'\
              '     if (!move_cursor(keyDict)) return l;\n' \
              '     for (;;)\n' \
              '     {\n' \
              '          l.append(gen_dict());\n' \
              '          if (!table.next()) { break; }\n' \
              '     }\n' \
              '     return l;\n' \
              '}\n'
        yield '\n'
        yield '\n'
        yield '\n'

    def gen_select_one(self):
        yield f'TableBase::dicttype Table{self.tablename}::select_one(const TableBase::dicttype& keyDict)\n'
        yield '{\n'
        yield '    if (!move_cursor(keyDict)) return {};\n'
        yield '    return gen_dict();\n'
        yield '}\n'
        yield '\n'
        yield '\n'
        yield '\n'

    def gen_select_with_no_con(self):
        yield f'TableBase::listdicttype Table{self.tablename}::select_items_with_no_cond()\n'
        yield '{\n' \
              '     py::list l;\n' \
              '    if (!table.lists()) return l;\n' \
              '    for (;;)\n' \
              '    {\n' \
              '        l.append(gen_dict());\n' \
              '        if (!table.next())\n' \
              '        { break; }\n' \
              '    }\n' \
              '    return l;\n' \
              '}\n'
        yield '\n'
        yield '\n'
        yield '\n'


    def gen_check_exist(self):
        indent = 0
        yield f'bool Table{self.tablename}::check_exist(const TableBase::dicttype& keyDict)\n'
        yield '{\n'
        indent+=1
        yield indent*self.i_size*' '+'if (!primary_constriant(keyDict)) { return false; }\n'
        yield indent*self.i_size*' '+f'return table.find_by_{self.pindex[1]}(\n'
        indent+=1
        for i,pk in enumerate(self.pindex[0]):
            if i<len(self.pindex[0])-1:
                yield indent*self.i_size*' '+f'{self.get_mapped(pk)}(keyDict[\"{pk}\"]),\n'
            else:
                yield indent * self.i_size * ' ' + f'{self.get_mapped(pk)}(keyDict["{pk}\"])\n'
        indent-=1
        yield indent * self.i_size * ' ' +');\n'
        # yield indent * self.i_size * ' ' +'return move_cursor(keyDict);\n'
        indent-=1
        yield '}\n'
        yield '\n'
        yield '\n'
        yield '\n'


    def gen_data_oper(self):
        yield f'TableBase::objecttype Table{self.tablename}::data_oper(const TableBase::dicttype& keyDict, const TableBase::dicttype& valueDict)\n' \
              '{\n' \
              '     dicttype temp;\n' \
              '     for (const auto &pr : valueDict)\n' \
              '     {\n' \
              '       temp[pr.first] =pr.second;\n' \
              '     }\n' \
              '     for (const auto &pr : keyDict)\n' \
              '     {\n' \
              '       temp[pr.first] =pr.second;\n' \
              '     }\n' \
              '     if (check_exist(keyDict))\n' \
              '     {\n' \
              '        if (!set_data(temp,false))\n' \
              '          {\n' \
              '             return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL);\n' \
              '        } else\n' \
              '        {\n' \
              '           return py::none();\n' \
              '        }\n' \
              '     }\n' \
              '     return insert_data(temp);\n' \
              '}\n'
        yield '\n'
        yield '\n'
        yield '\n'

    def gen_delete(self):
        yield f'TableBase::objecttype Table{self.tablename}::data_delete(const TableBase::dicttype& keyDict)\n'\
        '{\n'\
        '   if (!move_cursor(keyDict)) return TABLE_ERROR_MSG(tableError::RECORD_NOT_FOUND);\n'\
        '   for (;;)\n'\
        '   {\n'\
        '      table.erase();\n'\
        '      if (!table.next()) { break; }\n'\
        '   }\n'\
        '   return py::none();\n'\
        '}\n'
        yield '\n'
        yield '\n'
        yield '\n'


    def gen_cpp(self):
        yield from self.gen_include()
        yield from self.gen_insert()
        yield from self.gen_move_cursor()
        yield from self.gen_set()
        yield from self.gen_update()
        yield from self.gen_primary_constraints()
        yield from self.gen_gen_dict()
        yield from self.gen_select_items()
        yield from self.gen_select_with_no_con()
        yield from self.gen_select_one()
        yield from self.gen_check_exist()
        yield from self.gen_data_oper()
        yield from self.gen_delete()


    def gen_h(self):
        yield '#pragma once\n'
        yield f'#include \"tablebase.h\"\n'
        yield f'#include \"tradedb.h\"\n'
        yield f'class Table{self.tablename} : public TableBase {{\n'
        yield 'public:\n'
        yield f'   Table{self.tablename}(KsDbHandle database):table(database) {{}}\n'
        yield f'   virtual ~Table{self.tablename}() {{}}\n'
        yield '   virtual objecttype insert_data(const dicttype& valueDict) override;\n'
        yield '   virtual objecttype update_data(const dicttype& keyDict, const dicttype& valueDict) override;\n'
        yield '   virtual bool check_exist(const dicttype& keyDict) override;\n'
        yield '   virtual objecttype data_oper(const dicttype& keyDict, const dicttype& valueDict) override;\n'
        yield '   virtual dicttype select_one(const dicttype& keyDict) override;\n'
        yield '   virtual listdicttype select_items(const dicttype& keyDict) override;\n'
        yield '   virtual listdicttype select_items_with_no_cond() override;\n'
        yield '   virtual objecttype data_delete(const dicttype& keyDict) override;\n'
        yield '   virtual void dump() override { table.SaveTXT();}\n'
        yield 'private:\n'
        yield '   bool primary_constriant(const dicttype& keyDict);\n'
        yield '   bool set_data(const dicttype& valueDict, bool overwrite = true); \n'
        yield '   bool move_cursor(const dicttype& keyDict);\n'
        yield '   dicttype gen_dict();\n'
        yield f'   TB_{self.tablename} table;\n'
        yield '};\n'

    def gen_cl(self):
        indent = 0
        yield f'class {self.tablename}\n'
        yield '{\n'
        indent+=1
        pks = self.pks
        for v,k in zip(self.keydict.values(),self.keydict.keys()):
            yield indent*self.i_size*' '+ '{:10s}{:>20s};\n'.format(v,k)
        # for index in self.index:
        #     yield indent*self.i_size*' '+'tree<{}>{:>14s};\n'.format(index,index)
        for pindex in self.get_all_cls(pks):
            if not pindex:
                continue
            yield indent*self.i_size*' '+f'tree<{",".join(pindex)}>\t{"_".join(pindex)}_INDEX;\n'
        yield indent*self.i_size*' '+ f'unique tree<{",".join(self.pks)}>\t{"_".join(self.pks)}_INDEX;\n'
        yield '}\n'
        yield '\n'



    def write_file(self):
        open(self.cfile_path,'w').writelines(self.gen_cpp())
        open(self.hfile_path,'w').writelines(self.gen_h())
        #open(self.clfile_path,'a').writelines(self.gen_cl())







