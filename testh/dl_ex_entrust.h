#pragma once
#include "tablebase.h"
#include "tradedb.h"
class TableDL_EX_ENTRUST : public TableBase {
public:
   TableDL_EX_ENTRUST(KsDbHandle database):table(database) {}
   virtual ~TableDL_EX_ENTRUST() {}
   virtual py::handle insert_data(const dicttype& valueDict) override;
   virtual py::handle update_data(const dicttype& keyDict, const dicttype& valueDict) override;
   virtual bool check_exist(const dicttype& keyDict) override;
   virtual py::handle data_oper(const dicttype& keyDict, const dicttype& valueDict) override;
   virtual dicttype select_one(const dicttype& keyDict) override;
   virtual std::vector<dicttype> select_items(const dicttype& keyDict) override;
   virtual std::vector<dicttype> select_items_with_no_cond() override;
   virtual py::handle data_delete(const dicttype& keyDict) override;
   virtual void dump() override { table.SaveTXT();}
private:
   bool primary_constriant(const dicttype& keyDict);
   bool set_data(const dicttype& valueDict, bool overwrite = true); 
   bool move_cursor(const dicttype& keyDict);
   dicttype gen_dict();
   TB_DL_EX_ENTRUST table;
};
