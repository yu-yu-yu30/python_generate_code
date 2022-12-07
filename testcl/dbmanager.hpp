#pragma once
#include <memory>
#include <map>
#include <unordered_map>
#include <string>
#include "singleton.h"
#include "tradedb.h"
#include "tablebase.h"
#include "exrate_hks_reference.h"
#include "dl_algo_sub_order.h"
#include "dl_reckon.h"
#include "hedge_advice_rsk.h"
#include "risk_hedge_advice.h"
#include "risk_advice_range.h"
#include "stk_rsk_reflection.h"
#include "contract_exha_position.h"
#include "dl_manage_trade_intention.h"
#include "risk_advice_record.h"
#include "risk_pipe_line.h"
#include "risk_type_maintaining.h"
#include "risk_controlchild.h"
#include "hold_details.h"
#include "stock_index_future_tem.h"
#include "cust_marginrate.h"
#include "index_fee_and_addrec.h"
#include "otc_agent_contract_hold.h"
#include "dl_otc_done.h"
#include "acc_otc_ptflhld.h"
#include "otc_tem_fund_hold.h"
#include "dl_otc_entrust.h"
#include "original_order.h"
#include "hedge_instruction.h"
#include "hedge_advice.h"
#include "cust_stock_fee.h"
#include "tem_fund_hold.h"
#include "agent_contract_hold.h"
#include "system_monitor.h"
#include "md_quote.h"
#include "dl_ex_done_back.h"
#include "dl_ex_entrust.h"
#include "acc_matcho32.h"
#include "holder2seat.h"
#include "holder2cell.h"
#include "acc_ptflhld.h"
#include "otc_stock_index_future_tem.h"
#include "otc_hold_details.h"
#include "acc_ptflcap.h"
#include "acc_portfolio.h"
#include "agent_status.h"
#include "dl_ex_done.h"
#include "md_stock.h"


class DBManager final: public Singleton<DBManager> {

public:
    DBManager() {}
    virtual ~DBManager() {}
    bool init()
    {
        if (!m_database.Create(".", "tradedb", 1000000)) return false;
        create_table("EXRATE_HKS_REFERENCE", std::make_shared<TableEXRATE_HKS_REFERENCE>(m_database.GetDBHandle()));
        create_table("DL_ALGO_SUB_ORDER", std::make_shared<TableDL_ALGO_SUB_ORDER>(m_database.GetDBHandle()));
        create_table("DL_RECKON", std::make_shared<TableDL_RECKON>(m_database.GetDBHandle()));
        create_table("HEDGE_ADVICE_RSK", std::make_shared<TableHEDGE_ADVICE_RSK>(m_database.GetDBHandle()));
        create_table("RISK_HEDGE_ADVICE", std::make_shared<TableRISK_HEDGE_ADVICE>(m_database.GetDBHandle()));
        create_table("RISK_ADVICE_RANGE", std::make_shared<TableRISK_ADVICE_RANGE>(m_database.GetDBHandle()));
        create_table("STK_RSK_REFLECTION", std::make_shared<TableSTK_RSK_REFLECTION>(m_database.GetDBHandle()));
        create_table("CONTRACT_EXHA_POSITION", std::make_shared<TableCONTRACT_EXHA_POSITION>(m_database.GetDBHandle()));
        create_table("DL_MANAGE_TRADE_INTENTION", std::make_shared<TableDL_MANAGE_TRADE_INTENTION>(m_database.GetDBHandle()));
        create_table("RISK_ADVICE_RECORD", std::make_shared<TableRISK_ADVICE_RECORD>(m_database.GetDBHandle()));
        create_table("RISK_PIPE_LINE", std::make_shared<TableRISK_PIPE_LINE>(m_database.GetDBHandle()));
        create_table("RISK_TYPE_MAINTAINING", std::make_shared<TableRISK_TYPE_MAINTAINING>(m_database.GetDBHandle()));
        create_table("RISK_CONTROLCHILD", std::make_shared<TableRISK_CONTROLCHILD>(m_database.GetDBHandle()));
        create_table("HOLD_DETAILS", std::make_shared<TableHOLD_DETAILS>(m_database.GetDBHandle()));
        create_table("STOCK_INDEX_FUTURE_TEM", std::make_shared<TableSTOCK_INDEX_FUTURE_TEM>(m_database.GetDBHandle()));
        create_table("CUST_MARGINRATE", std::make_shared<TableCUST_MARGINRATE>(m_database.GetDBHandle()));
        create_table("INDEX_FEE_AND_ADDREC", std::make_shared<TableINDEX_FEE_AND_ADDREC>(m_database.GetDBHandle()));
        create_table("OTC_AGENT_CONTRACT_HOLD", std::make_shared<TableOTC_AGENT_CONTRACT_HOLD>(m_database.GetDBHandle()));
        create_table("DL_OTC_DONE", std::make_shared<TableDL_OTC_DONE>(m_database.GetDBHandle()));
        create_table("ACC_OTC_PTFLHLD", std::make_shared<TableACC_OTC_PTFLHLD>(m_database.GetDBHandle()));
        create_table("OTC_TEM_FUND_HOLD", std::make_shared<TableOTC_TEM_FUND_HOLD>(m_database.GetDBHandle()));
        create_table("DL_OTC_ENTRUST", std::make_shared<TableDL_OTC_ENTRUST>(m_database.GetDBHandle()));
        create_table("ORIGINAL_ORDER", std::make_shared<TableORIGINAL_ORDER>(m_database.GetDBHandle()));
        create_table("HEDGE_INSTRUCTION", std::make_shared<TableHEDGE_INSTRUCTION>(m_database.GetDBHandle()));
        create_table("HEDGE_ADVICE", std::make_shared<TableHEDGE_ADVICE>(m_database.GetDBHandle()));
        create_table("CUST_STOCK_FEE", std::make_shared<TableCUST_STOCK_FEE>(m_database.GetDBHandle()));
        create_table("TEM_FUND_HOLD", std::make_shared<TableTEM_FUND_HOLD>(m_database.GetDBHandle()));
        create_table("AGENT_CONTRACT_HOLD", std::make_shared<TableAGENT_CONTRACT_HOLD>(m_database.GetDBHandle()));
        create_table("SYSTEM_MONITOR", std::make_shared<TableSYSTEM_MONITOR>(m_database.GetDBHandle()));
        create_table("MD_QUOTE", std::make_shared<TableMD_QUOTE>(m_database.GetDBHandle()));
        create_table("DL_EX_DONE_BACK", std::make_shared<TableDL_EX_DONE_BACK>(m_database.GetDBHandle()));
        create_table("DL_EX_ENTRUST", std::make_shared<TableDL_EX_ENTRUST>(m_database.GetDBHandle()));
        create_table("ACC_MATCHO32", std::make_shared<TableACC_MATCHO32>(m_database.GetDBHandle()));
        create_table("HOLDER2SEAT", std::make_shared<TableHOLDER2SEAT>(m_database.GetDBHandle()));
        create_table("HOLDER2CELL", std::make_shared<TableHOLDER2CELL>(m_database.GetDBHandle()));
        create_table("ACC_PTFLHLD", std::make_shared<TableACC_PTFLHLD>(m_database.GetDBHandle()));
        create_table("OTC_STOCK_INDEX_FUTURE_TEM", std::make_shared<TableOTC_STOCK_INDEX_FUTURE_TEM>(m_database.GetDBHandle()));
        create_table("OTC_HOLD_DETAILS", std::make_shared<TableOTC_HOLD_DETAILS>(m_database.GetDBHandle()));
        create_table("ACC_PTFLCAP", std::make_shared<TableACC_PTFLCAP>(m_database.GetDBHandle()));
        create_table("ACC_PORTFOLIO", std::make_shared<TableACC_PORTFOLIO>(m_database.GetDBHandle()));
        create_table("AGENT_STATUS", std::make_shared<TableAGENT_STATUS>(m_database.GetDBHandle()));
        create_table("DL_EX_DONE", std::make_shared<TableDL_EX_DONE>(m_database.GetDBHandle()));
        create_table("MD_STOCK", std::make_shared<TableMD_STOCK>(m_database.GetDBHandle()));

        return true;
    }
    inline void create_table(const std::string& tableName, std::shared_ptr<TableBase> tableManupulator)
    {
        m_distributor.emplace(tableName, tableManupulator);
    }

    inline bool is_in_transaction()  { return in_transaction;}

    void begin_transaction()
    {
        in_transaction = true;
        m_database.BeginTransaction();
    }

    void commit()
    {
        m_database.CommitTransaction();
        in_transaction = false;
    }

    void rollback()
    {
        m_database.RollbackTransaction();
        in_transaction = false;
    }

    DB_tradedb& get_database()
    {
        return m_database;
    }

    std::shared_ptr<TableBase> get_table(const std::string& tableName)
    {
        auto ite = m_distributor.find(tableName);
        if (ite != m_distributor.cend()) {
            return ite->second;
        }
        return nullptr;
    }

    void create_dump()
    {
        for (const auto& pr: m_distributor)
        {
            pr.second->dump();
        }
    }

private:
    DB_tradedb m_database;
    std::unordered_map<std::string, std::shared_ptr<TableBase>> m_distributor;
    bool in_transaction = false;
};

#define DBMIns DBManager::instance()