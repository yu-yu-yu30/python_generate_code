#include "dl_ex_done.h"


TableBase::objecttype TableDL_EX_DONE::insert_data(const TableBase::dicttype& valueDict)
{
     if (!primary_constriant(valueDict)) { return TABLE_ERROR_MSG(tableError::MISSING_PRIMRARY_KEY); }

     table.newrecord();

     if (!set_data(valueDict)) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }
          if (!table.append(
           PyStr_To_CString(valueDict["DONE_DATE"]),
          PyStr_To_CString(valueDict["TRADING_CODE"])
     )) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }
     return py::none();
}


bool TableDL_EX_DONE::move_cursor(const TableBase::dicttype& keyDict)
{
     auto iteDONE_DATE = keyDict.contains("DONE_DATE");
     auto iteTRADING_CODE = keyDict.contains("TRADING_CODE");
     if (iteDONE_DATE&&!iteTRADING_CODE)
     {
          return table.find_by_DONE_DATE_INDEX(
               PyStr_To_CString(keyDict["DONE_DATE"])
          );
     }
     if (iteTRADING_CODE&&!iteDONE_DATE)
     {
          return table.find_by_TRADING_CODE_INDEX(
               PyStr_To_CString(keyDict["TRADING_CODE"])
          );
     }
      return false;
}


bool TableDL_EX_DONE::set_data(const TableBase::dicttype& valueDict,bool overwrite)
{
     auto success = true;
     auto oper = true;
     if (valueDict.contains("CONTRACT_CODE"))
      {
          oper = table.set_CONTRACT_CODE(const_cast<char *>(PyStr_To_CString(valueDict["CONTRACT_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_CONTRACT_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("CELL_ID"))
      {
          oper = table.set_CELL_ID(py::cast<long>(valueDict["CELL_ID"]));
     }
     else
     {
          if (overwrite) oper = table.set_CELL_ID(0);
     }
     success = success && oper;

     if (valueDict.contains("HOLDER_ACC"))
      {
          oper = table.set_HOLDER_ACC(const_cast<char *>(PyStr_To_CString(valueDict["HOLDER_ACC"])));
     }
     else
     {
          if (overwrite) oper = table.set_HOLDER_ACC(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("ENT_NO"))
      {
          oper = table.set_ENT_NO(const_cast<char *>(PyStr_To_CString(valueDict["ENT_NO"])));
     }
     else
     {
          if (overwrite) oper = table.set_ENT_NO(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("DONE_PRICE"))
      {
          oper = table.set_DONE_PRICE(py::cast<double>(valueDict["DONE_PRICE"]));
     }
     else
     {
          if (overwrite) oper = table.set_DONE_PRICE(0.0);
     }
     success = success && oper;

     if (valueDict.contains("FUND_ID"))
      {
          oper = table.set_FUND_ID(py::cast<long>(valueDict["FUND_ID"]));
     }
     else
     {
          if (overwrite) oper = table.set_FUND_ID(0);
     }
     success = success && oper;

     if (valueDict.contains("DONE_AMT"))
      {
          oper = table.set_DONE_AMT(py::cast<double>(valueDict["DONE_AMT"]));
     }
     else
     {
          if (overwrite) oper = table.set_DONE_AMT(0.0);
     }
     success = success && oper;

     if (valueDict.contains("INSTRUCT_ID"))
      {
          oper = table.set_INSTRUCT_ID(const_cast<char *>(PyStr_To_CString(valueDict["INSTRUCT_ID"])));
     }
     else
     {
          if (overwrite) oper = table.set_INSTRUCT_ID(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("KP_TP"))
      {
          oper = table.set_KP_TP(const_cast<char *>(PyStr_To_CString(valueDict["KP_TP"])));
     }
     else
     {
          if (overwrite) oper = table.set_KP_TP(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("SEAT_ID"))
      {
          oper = table.set_SEAT_ID(const_cast<char *>(PyStr_To_CString(valueDict["SEAT_ID"])));
     }
     else
     {
          if (overwrite) oper = table.set_SEAT_ID(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("STK_CODE"))
      {
          oper = table.set_STK_CODE(const_cast<char *>(PyStr_To_CString(valueDict["STK_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_STK_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("PORTFOLIO_ID"))
      {
          oper = table.set_PORTFOLIO_ID(py::cast<long>(valueDict["PORTFOLIO_ID"]));
     }
     else
     {
          if (overwrite) oper = table.set_PORTFOLIO_ID(0);
     }
     success = success && oper;

     if (valueDict.contains("OTC_CODE"))
      {
          oper = table.set_OTC_CODE(const_cast<char *>(PyStr_To_CString(valueDict["OTC_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_OTC_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("FEE"))
      {
          oper = table.set_FEE(py::cast<double>(valueDict["FEE"]));
     }
     else
     {
          if (overwrite) oper = table.set_FEE(0.0);
     }
     success = success && oper;

     if (valueDict.contains("UN_CODE"))
      {
          oper = table.set_UN_CODE(const_cast<char *>(PyStr_To_CString(valueDict["UN_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_UN_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("DONE_TIME"))
      {
          oper = table.set_DONE_TIME(const_cast<char *>(PyStr_To_CString(valueDict["DONE_TIME"])));
     }
     else
     {
          if (overwrite) oper = table.set_DONE_TIME(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("BS_TP"))
      {
          oper = table.set_BS_TP(const_cast<char *>(PyStr_To_CString(valueDict["BS_TP"])));
     }
     else
     {
          if (overwrite) oper = table.set_BS_TP(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("MK_CODE"))
      {
          oper = table.set_MK_CODE(const_cast<char *>(PyStr_To_CString(valueDict["MK_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_MK_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("ORIGIN_ENTRUST_CODE"))
      {
          oper = table.set_ORIGIN_ENTRUST_CODE(const_cast<char *>(PyStr_To_CString(valueDict["ORIGIN_ENTRUST_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_ORIGIN_ENTRUST_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("HENTRUST_CODE"))
      {
          oper = table.set_HENTRUST_CODE(const_cast<char *>(PyStr_To_CString(valueDict["HENTRUST_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_HENTRUST_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("DONE_CAP"))
      {
          oper = table.set_DONE_CAP(py::cast<double>(valueDict["DONE_CAP"]));
     }
     else
     {
          if (overwrite) oper = table.set_DONE_CAP(0.0);
     }
     success = success && oper;

     if (valueDict.contains("O32ENTRUST_NO"))
      {
          oper = table.set_O32ENTRUST_NO(const_cast<char *>(PyStr_To_CString(valueDict["O32ENTRUST_NO"])));
     }
     else
     {
          if (overwrite) oper = table.set_O32ENTRUST_NO(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("INTENTION_CODE"))
      {
          oper = table.set_INTENTION_CODE(const_cast<char *>(PyStr_To_CString(valueDict["INTENTION_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_INTENTION_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     if (valueDict.contains("HD_ENTRUST_CODE"))
      {
          oper = table.set_HD_ENTRUST_CODE(const_cast<char *>(PyStr_To_CString(valueDict["HD_ENTRUST_CODE"])));
     }
     else
     {
          if (overwrite) oper = table.set_HD_ENTRUST_CODE(const_cast<char *>(""));
     }
     success = success && oper;

     return success;
}


TableBase::objecttype TableDL_EX_DONE::update_data(const TableBase::dicttype& keyDict, const TableBase::dicttype& valueDict)
{
     if (!keyDict.contains("TRADING_CODE"))
     {
          return TABLE_ERROR_MSG(tableError::MISSING_PRIMRARY_KEY);
     }
     if (!table.find_by_TRADING_CODE_INDEX(
          PyStr_To_CString(keyDict["TRADING_CODE"])
     ))  { return  TABLE_ERROR_MSG(tableError::RECORD_NOT_FOUND); }
     if (!set_data(valueDict, false)) { return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL); }
     return py::none();
}



bool TableDL_EX_DONE::primary_constriant(const dicttype& keyDict)
{
     return (keyDict.contains("TRADING_CODE"));
}


TableBase::dicttype TableDL_EX_DONE::gen_dict()
{
     dicttype d;
     d["DONE_DATE"] = py::cast(table.get_DONE_DATE());
     d["MK_CODE"] = py::cast(table.get_MK_CODE());
     d["TRADING_CODE"] = py::cast(table.get_TRADING_CODE());
     d["OTC_CODE"] = py::cast(table.get_OTC_CODE());
     d["ENT_NO"] = py::cast(table.get_ENT_NO());
     d["HD_ENTRUST_CODE"] = py::cast(table.get_HD_ENTRUST_CODE());
     d["HENTRUST_CODE"] = py::cast(table.get_HENTRUST_CODE());
     d["ORIGIN_ENTRUST_CODE"] = py::cast(table.get_ORIGIN_ENTRUST_CODE());
     d["O32ENTRUST_NO"] = py::cast(table.get_O32ENTRUST_NO());
     d["INSTRUCT_ID"] = py::cast(table.get_INSTRUCT_ID());
     d["SEAT_ID"] = py::cast(table.get_SEAT_ID());
     d["HOLDER_ACC"] = py::cast(table.get_HOLDER_ACC());
     d["FUND_ID"] = py::cast(table.get_FUND_ID());
     d["CELL_ID"] = py::cast(table.get_CELL_ID());
     d["PORTFOLIO_ID"] = py::cast(table.get_PORTFOLIO_ID());
     d["STK_CODE"] = py::cast(table.get_STK_CODE());
     d["DONE_AMT"] = py::cast(table.get_DONE_AMT());
     d["DONE_PRICE"] = py::cast(table.get_DONE_PRICE());
     d["DONE_CAP"] = py::cast(table.get_DONE_CAP());
     d["KP_TP"] = py::cast(table.get_KP_TP());
     d["BS_TP"] = py::cast(table.get_BS_TP());
     d["DONE_TIME"] = py::cast(table.get_DONE_TIME());
     d["FEE"] = py::cast(table.get_FEE());
     d["UN_CODE"] = py::cast(table.get_UN_CODE());
     d["CONTRACT_CODE"] = py::cast(table.get_CONTRACT_CODE());
     d["INTENTION_CODE"] = py::cast(table.get_INTENTION_CODE());
     return d;
}



TableBase::listdicttype TableDL_EX_DONE::select_items(const TableBase::dicttype& keyDict)
{
     py::list l;
     if (!move_cursor(keyDict)) return l;
     for (;;)
     {
          l.append(gen_dict());
          if (!table.next()) { break; }
     }
     return l;
}



TableBase::listdicttype TableDL_EX_DONE::select_items_with_no_cond()
{
     py::list l;
    if (!table.lists()) return l;
    for (;;)
    {
        l.append(gen_dict());
        if (!table.next())
        { break; }
    }
    return l;
}



TableBase::dicttype TableDL_EX_DONE::select_one(const TableBase::dicttype& keyDict)
{
    if (!move_cursor(keyDict)) return {};
    return gen_dict();
}



bool TableDL_EX_DONE::check_exist(const TableBase::dicttype& keyDict)
{
     if (!primary_constriant(keyDict)) { return false; }
     return table.find_by_TRADING_CODE_INDEX(
          PyStr_To_CString(keyDict["TRADING_CODE"])
     );
}



TableBase::objecttype TableDL_EX_DONE::data_oper(const TableBase::dicttype& keyDict, const TableBase::dicttype& valueDict)
{
     dicttype temp;
     for (const auto &pr : valueDict)
     {
       temp[pr.first] =pr.second;
     }
     for (const auto &pr : keyDict)
     {
       temp[pr.first] =pr.second;
     }
     if (check_exist(keyDict))
     {
        if (!set_data(temp,false))
          {
             return TABLE_ERROR_MSG(tableError::SET_DATA_FAIL);
        } else
        {
           return py::none();
        }
     }
     return insert_data(temp);
}



TableBase::objecttype TableDL_EX_DONE::data_delete(const TableBase::dicttype& keyDict)
{
   if (!move_cursor(keyDict)) return TABLE_ERROR_MSG(tableError::RECORD_NOT_FOUND);
   for (;;)
   {
      table.erase();
      if (!table.next()) { break; }
   }
   return py::none();
}



