#pragma once

#include <cstdint>

namespace janus {

//=============================================================================
// TPC-C Transaction Types
//=============================================================================
constexpr uint32_t LUIGI_TXN_MICRO = 0;
constexpr uint32_t LUIGI_TXN_NEW_ORDER = 1;
constexpr uint32_t LUIGI_TXN_PAYMENT = 2;
constexpr uint32_t LUIGI_TXN_ORDER_STATUS = 3;
constexpr uint32_t LUIGI_TXN_DELIVERY = 4;
constexpr uint32_t LUIGI_TXN_STOCK_LEVEL = 5;

//=============================================================================
// TPC-C Table IDs
//=============================================================================
constexpr uint16_t TPCC_TB_WAREHOUSE = 10;
constexpr uint16_t TPCC_TB_DISTRICT = 11;
constexpr uint16_t TPCC_TB_CUSTOMER = 12;
constexpr uint16_t TPCC_TB_HISTORY = 13;
constexpr uint16_t TPCC_TB_ORDER = 14;
constexpr uint16_t TPCC_TB_NEW_ORDER = 15;
constexpr uint16_t TPCC_TB_ITEM = 16;
constexpr uint16_t TPCC_TB_STOCK = 17;
constexpr uint16_t TPCC_TB_ORDER_LINE = 18;

//=============================================================================
// TPC-C Variable IDs (for working_set parameters)
// Following Tiga's convention
//=============================================================================

// Common variables
constexpr int32_t TPCC_VAR_W_ID = 1001;
constexpr int32_t TPCC_VAR_D_ID = 1003;
constexpr int32_t TPCC_VAR_C_ID = 1007;
constexpr int32_t TPCC_VAR_C_LAST = 1004;
constexpr int32_t TPCC_VAR_C_W_ID = 1005;
constexpr int32_t TPCC_VAR_C_D_ID = 1006;

// NewOrder variables
constexpr int32_t TPCC_VAR_OL_CNT = 1015;
constexpr int32_t TPCC_VAR_O_ALL_LOCAL = 1016;
constexpr int32_t TPCC_VAR_O_CARRIER_ID = 1012;

// Payment variables
constexpr int32_t TPCC_VAR_H_AMOUNT = 1002;
constexpr int32_t TPCC_VAR_H_DATE = 1081;

// StockLevel variables
constexpr int32_t TPCC_VAR_THRESHOLD = 1060;

// District variables
constexpr int32_t TPCC_VAR_D_NEXT_O_ID = 1014;
constexpr int32_t TPCC_VAR_D_TAX = 1037;
constexpr int32_t TPCC_VAR_D_YTD = 1039;

// Warehouse variables
constexpr int32_t TPCC_VAR_W_TAX = 1038;
constexpr int32_t TPCC_VAR_W_YTD = 1036;

// Customer variables
constexpr int32_t TPCC_VAR_C_DISCOUNT = 1049;
constexpr int32_t TPCC_VAR_C_BALANCE = 1050;
constexpr int32_t TPCC_VAR_C_YTD_PAYMENT = 1054;
constexpr int32_t TPCC_VAR_C_PAYMENT_CNT = 1056;
constexpr int32_t TPCC_VAR_C_DELIVERY_CNT = 1062;

// Per-item variables (use macros for array access)
#define TPCC_VAR_I_ID(i) (100000 + (i))
#define TPCC_VAR_I_PRICE(i) (102000 + (i))
#define TPCC_VAR_S_W_ID(i) (106000 + (i))
#define TPCC_VAR_S_QUANTITY(i) (119000 + (i))
#define TPCC_VAR_S_REMOTE_CNT(i) (109000 + (i))
#define TPCC_VAR_OL_QUANTITY(i) (108000 + (i))
#define TPCC_VAR_OL_AMOUNT(i) (118000 + (i))
#define TPCC_VAR_OL_DIST_INFO(i) (116000 + (i))

//=============================================================================
// TPC-C Configuration Constants
//=============================================================================
constexpr uint32_t TPCC_DISTRICTS_PER_WAREHOUSE = 10;
constexpr uint32_t TPCC_CUSTOMERS_PER_DISTRICT = 3000;
constexpr uint32_t TPCC_ITEMS_TOTAL = 100000;
constexpr uint32_t TPCC_INITIAL_ORDERS_PER_DISTRICT = 3000;
constexpr uint32_t TPCC_INITIAL_NEW_ORDERS = 900;

// TPC-C spec: 5-15 order lines per order
constexpr uint32_t TPCC_MIN_OL_CNT = 5;
constexpr uint32_t TPCC_MAX_OL_CNT = 15;

// TPC-C spec: 1% remote items, 15% remote payment
constexpr uint32_t TPCC_REMOTE_ITEM_PCT = 1;
constexpr uint32_t TPCC_REMOTE_PAYMENT_PCT = 15;

// TPC-C spec: 60% customer lookup by lastname
constexpr uint32_t TPCC_CUSTOMER_BY_NAME_PCT = 60;

} // namespace janus
