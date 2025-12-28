#pragma once

/**
 * TPCCTxnGenerator - TPC-C benchmark transaction generator for Luigi
 *
 * Generates TPC-C transaction requests (New Order, Payment, Order Status,
 * Delivery, Stock Level) in stored-procedure style for Luigi's dispatch.
 *
 * Modeled after Tiga's TPCCTxnGenerator with simplifications for Mako.
 */

#include "txn_generator.h"

#include <algorithm>
#include <cmath>
#include <set>
#include <string>
#include <vector>

namespace janus {

// TPC-C Table IDs (should match Mako's TPCC tables)
constexpr uint16_t TPCC_TABLE_WAREHOUSE = 1;
constexpr uint16_t TPCC_TABLE_DISTRICT = 2;
constexpr uint16_t TPCC_TABLE_CUSTOMER = 3;
constexpr uint16_t TPCC_TABLE_HISTORY = 4;
constexpr uint16_t TPCC_TABLE_NEW_ORDER = 5;
constexpr uint16_t TPCC_TABLE_ORDER = 6;
constexpr uint16_t TPCC_TABLE_ORDER_LINE = 7;
constexpr uint16_t TPCC_TABLE_ITEM = 8;
constexpr uint16_t TPCC_TABLE_STOCK = 9;

// TPC-C Variable IDs (matching Mako's TPCC definitions)
constexpr int32_t TPCC_VAR_W_ID = 1001;
constexpr int32_t TPCC_VAR_D_ID = 1003;
constexpr int32_t TPCC_VAR_C_ID = 1004;
constexpr int32_t TPCC_VAR_C_W_ID = 1005;
constexpr int32_t TPCC_VAR_C_D_ID = 1006;
constexpr int32_t TPCC_VAR_H_AMOUNT = 1002;
constexpr int32_t TPCC_VAR_H_KEY = 1008;
constexpr int32_t TPCC_VAR_O_ID = 1011;
constexpr int32_t TPCC_VAR_OL_CNT = 1012;
constexpr int32_t TPCC_VAR_O_CARRIER_ID = 1013;
constexpr int32_t TPCC_VAR_O_ALL_LOCAL = 1014;
constexpr int32_t TPCC_VAR_C_LAST = 1015;
constexpr int32_t TPCC_VAR_THRESHOLD = 1016;

// Dynamic variable IDs for order lines
inline int32_t TPCC_VAR_I_ID(int i) { return 2000 + i; }
inline int32_t TPCC_VAR_S_W_ID(int i) { return 3000 + i; }
inline int32_t TPCC_VAR_OL_NUMBER(int i) { return 4000 + i; }
inline int32_t TPCC_VAR_OL_QUANTITY(int i) { return 5000 + i; }
inline int32_t TPCC_VAR_OL_DELIVER_D(int i) { return 6000 + i; }
inline int32_t TPCC_VAR_OL_DIST_INFO(int i) { return 7000 + i; }
inline int32_t TPCC_VAR_S_REMOTE_CNT(int i) { return 8000 + i; }

class TPCCTxnGenerator : public LuigiTxnGenerator {
public:
  TPCCTxnGenerator(const TxnGeneratorConfig &config)
      : LuigiTxnGenerator(config) {}

  std::string RTTI() override { return "TPCCTxnGenerator"; }

  void GetTxnReq(LuigiTxnRequest *req, uint32_t req_id,
                 uint32_t client_id) override {
    req->client_id = client_id;
    req->req_id = req_id;

    // Clear previous state
    req->working_set.clear();
    req->target_shards.clear();
    req->ops.clear();

    // Select transaction type based on weights
    double r = RandomDouble();
    double cumulative = 0.0;

    cumulative += config_.new_order_weight;
    if (r < cumulative) {
      GetNewOrderTxn(req);
      return;
    }

    cumulative += config_.payment_weight;
    if (r < cumulative) {
      GetPaymentTxn(req);
      return;
    }

    cumulative += config_.order_status_weight;
    if (r < cumulative) {
      GetOrderStatusTxn(req);
      return;
    }

    cumulative += config_.delivery_weight;
    if (r < cumulative) {
      GetDeliveryTxn(req);
      return;
    }

    // Default: Stock Level
    GetStockLevelTxn(req);
  }

private:
  /**
   * Generate New Order transaction.
   * Most complex TPC-C transaction, involves multiple tables and potentially
   * multiple warehouses (remote stock).
   */
  void GetNewOrderTxn(LuigiTxnRequest *req) {
    req->txn_type = LUIGI_TXN_TPCC_NEW_ORDER;

    // Select warehouse based on client_id for locality
    int32_t home_w_id = req->client_id % config_.num_warehouses;
    int32_t d_id = (req->client_id / config_.num_warehouses) %
                   config_.num_districts_per_wh;
    int32_t c_id = NURand(1022, 0, config_.num_customers_per_district - 1);

    // Number of order lines: 5-15
    int32_t ol_cnt = RandomInt(5, 16);

    // Add to working set
    req->working_set[TPCC_VAR_W_ID] = std::to_string(home_w_id);
    req->working_set[TPCC_VAR_D_ID] = std::to_string(d_id);
    req->working_set[TPCC_VAR_C_ID] = std::to_string(c_id);
    req->working_set[TPCC_VAR_OL_CNT] = std::to_string(ol_cnt);
    req->working_set[TPCC_VAR_O_CARRIER_ID] = "0";

    // Track target shards
    uint32_t home_shard = WarehouseToShard(home_w_id);
    req->target_shards.insert(home_shard);

    // Create ops for warehouse, district, customer
    std::string wh_key = "wh_" + std::to_string(home_w_id);
    std::string dist_key =
        "dist_" + std::to_string(home_w_id) + "_" + std::to_string(d_id);
    std::string cust_key = "cust_" + std::to_string(home_w_id) + "_" +
                           std::to_string(d_id) + "_" + std::to_string(c_id);

    req->ops.push_back(MakeOp(TPCC_TABLE_WAREHOUSE, 0, wh_key, ""));   // Read
    req->ops.push_back(MakeOp(TPCC_TABLE_DISTRICT, 1, dist_key, "1")); // Write
    req->ops.push_back(MakeOp(TPCC_TABLE_CUSTOMER, 0, cust_key, ""));  // Read

    // Generate order lines
    std::set<int32_t> used_items;
    bool all_local = true;

    for (int32_t i = 0; i < ol_cnt; i++) {
      // Select unique item
      int32_t i_id;
      do {
        i_id = RandomInt(0, config_.num_items - 1);
      } while (used_items.find(i_id) != used_items.end());
      used_items.insert(i_id);

      // Determine supply warehouse (configurable remote %)
      int32_t supply_w_id;
      if (config_.num_warehouses > 1 &&
          RandomInt(0, 100) < static_cast<int32_t>(config_.remote_item_pct)) {
        // Remote supply
        supply_w_id = RandomInt(0, config_.num_warehouses - 2);
        if (supply_w_id >= home_w_id)
          supply_w_id++;
        all_local = false;
      } else {
        supply_w_id = home_w_id;
      }

      int32_t quantity = RandomInt(1, 11);

      req->working_set[TPCC_VAR_I_ID(i)] = std::to_string(i_id);
      req->working_set[TPCC_VAR_S_W_ID(i)] = std::to_string(supply_w_id);
      req->working_set[TPCC_VAR_OL_QUANTITY(i)] = std::to_string(quantity);

      // Track shard for supply warehouse
      uint32_t supply_shard = WarehouseToShard(supply_w_id);
      req->target_shards.insert(supply_shard);

      // Create ops for item and stock
      std::string item_key = "item_" + std::to_string(i_id);
      std::string stock_key =
          "stock_" + std::to_string(supply_w_id) + "_" + std::to_string(i_id);
      req->ops.push_back(MakeOp(TPCC_TABLE_ITEM, 0, item_key, "")); // Read
      req->ops.push_back(MakeOp(TPCC_TABLE_STOCK, 1, stock_key,
                                std::to_string(quantity))); // Write
    }

    req->working_set[TPCC_VAR_O_ALL_LOCAL] = all_local ? "1" : "0";
  }

  /**
   * Generate Payment transaction.
   * Updates warehouse, district, and customer with payment amount.
   * May involve remote customer lookup (15% chance).
   */
  void GetPaymentTxn(LuigiTxnRequest *req) {
    req->txn_type = LUIGI_TXN_TPCC_PAYMENT;

    int32_t home_w_id = req->client_id % config_.num_warehouses;
    int32_t d_id = (req->client_id / config_.num_warehouses) %
                   config_.num_districts_per_wh;

    // Customer warehouse and district (configurable remote %)
    int32_t c_w_id, c_d_id;
    if (config_.num_warehouses > 1 &&
        RandomInt(0, 100) < static_cast<int32_t>(config_.remote_payment_pct)) {
      c_w_id = RandomInt(0, config_.num_warehouses - 2);
      if (c_w_id >= home_w_id)
        c_w_id++;
      c_d_id = RandomInt(0, config_.num_districts_per_wh - 1);
    } else {
      c_w_id = home_w_id;
      c_d_id = d_id;
    }

    double h_amount = 1.0 + RandomDouble() * 4999.0; // $1-$5000

    req->working_set[TPCC_VAR_W_ID] = std::to_string(home_w_id);
    req->working_set[TPCC_VAR_D_ID] = std::to_string(d_id);
    req->working_set[TPCC_VAR_C_W_ID] = std::to_string(c_w_id);
    req->working_set[TPCC_VAR_C_D_ID] = std::to_string(c_d_id);
    req->working_set[TPCC_VAR_H_AMOUNT] = std::to_string(h_amount);
    req->working_set[TPCC_VAR_H_KEY] = std::to_string(RandomInt(0, INT32_MAX));

    // 60% lookup by last name
    int32_t c_id;
    if (RandomInt(0, 100) < 60) {
      std::string c_last = GenerateLastName(NURand(255, 0, 999));
      req->working_set[TPCC_VAR_C_LAST] = c_last;
      c_id = 0; // Use 0 as placeholder for last name lookup
    } else {
      c_id = NURand(1022, 0, config_.num_customers_per_district - 1);
      req->working_set[TPCC_VAR_C_ID] = std::to_string(c_id);
    }

    // Target shards
    req->target_shards.insert(WarehouseToShard(home_w_id));
    req->target_shards.insert(WarehouseToShard(c_w_id));
    req->working_set[TPCC_VAR_H_AMOUNT] = std::to_string(h_amount); // Insert

    // Create ops for payment transaction
    std::string wh_key = "wh_" + std::to_string(home_w_id);
    std::string dist_key =
        "dist_" + std::to_string(home_w_id) + "_" + std::to_string(d_id);
    std::string cust_key = "cust_" + std::to_string(c_w_id) + "_" +
                           std::to_string(c_d_id) + "_" + std::to_string(c_id);
    std::string hist_key = "hist_" + std::to_string(RandomInt(0, INT32_MAX));

    req->ops.push_back(MakeOp(TPCC_TABLE_WAREHOUSE, 1, wh_key,
                              std::to_string(h_amount))); // Write
    req->ops.push_back(MakeOp(TPCC_TABLE_DISTRICT, 1, dist_key,
                              std::to_string(h_amount))); // Write
    req->ops.push_back(MakeOp(TPCC_TABLE_CUSTOMER, 1, cust_key,
                              std::to_string(h_amount))); // Write
    req->ops.push_back(MakeOp(TPCC_TABLE_HISTORY, 1, hist_key,
                              std::to_string(h_amount))); // Insert
  }

  /**
   * Generate Order Status transaction (read-only).
   */
  void GetOrderStatusTxn(LuigiTxnRequest *req) {
    req->txn_type = LUIGI_TXN_TPCC_ORDER_STATUS;

    int32_t w_id = req->client_id % config_.num_warehouses;
    int32_t d_id = (req->client_id / config_.num_warehouses) %
                   config_.num_districts_per_wh;
    int32_t c_id = NURand(1022, 0, config_.num_customers_per_district - 1);

    req->working_set[TPCC_VAR_W_ID] = std::to_string(w_id);
    req->working_set[TPCC_VAR_D_ID] = std::to_string(d_id);
    req->working_set[TPCC_VAR_C_ID] = std::to_string(c_id);

    // 60% by last name
    if (RandomInt(0, 100) < 60) {
      req->working_set[TPCC_VAR_C_LAST] = GenerateLastName(NURand(255, 0, 999));
    }

    req->target_shards.insert(WarehouseToShard(w_id));

    // Create read ops for customer and order
    std::string cust_key = "cust_" + std::to_string(w_id) + "_" +
                           std::to_string(d_id) + "_" + std::to_string(c_id);
    std::string order_key =
        "order_" + std::to_string(w_id) + "_" + std::to_string(d_id);
    req->ops.push_back(MakeOp(TPCC_TABLE_CUSTOMER, 0, cust_key, "")); // Read
    req->ops.push_back(MakeOp(TPCC_TABLE_ORDER, 0, order_key, ""));   // Read
  }

  /**
   * Generate Delivery transaction.
   * Processes oldest new orders for all districts.
   */
  void GetDeliveryTxn(LuigiTxnRequest *req) {
    req->txn_type = LUIGI_TXN_TPCC_DELIVERY;

    int32_t w_id = req->client_id % config_.num_warehouses;
    int32_t carrier_id = RandomInt(1, 11);

    req->working_set[TPCC_VAR_W_ID] = std::to_string(w_id);
    req->working_set[TPCC_VAR_O_CARRIER_ID] = std::to_string(carrier_id);

    req->target_shards.insert(WarehouseToShard(w_id));

    // Process all districts - create ops for each
    for (int32_t d_id = 0;
         d_id < static_cast<int32_t>(config_.num_districts_per_wh); d_id++) {
      req->working_set[TPCC_VAR_D_ID] = std::to_string(d_id);

      // Create ops for new_order and order updates
      std::string new_order_key =
          "new_order_" + std::to_string(w_id) + "_" + std::to_string(d_id);
      std::string order_key =
          "order_" + std::to_string(w_id) + "_" + std::to_string(d_id);
      req->ops.push_back(
          MakeOp(TPCC_TABLE_NEW_ORDER, 1, new_order_key, "")); // Delete/Update
      req->ops.push_back(MakeOp(TPCC_TABLE_ORDER, 1, order_key,
                                std::to_string(carrier_id))); // Update
    }
  }

  /**
   * Generate Stock Level transaction (read-only).
   */
  void GetStockLevelTxn(LuigiTxnRequest *req) {
    req->txn_type = LUIGI_TXN_TPCC_STOCK_LEVEL;

    int32_t w_id = req->client_id % config_.num_warehouses;
    int32_t d_id = (req->client_id / config_.num_warehouses) %
                   config_.num_districts_per_wh;
    int32_t threshold = RandomInt(10, 21);

    req->working_set[TPCC_VAR_W_ID] = std::to_string(w_id);
    req->working_set[TPCC_VAR_D_ID] = std::to_string(d_id);
    req->working_set[TPCC_VAR_THRESHOLD] = std::to_string(threshold);

    req->target_shards.insert(WarehouseToShard(w_id));

    // Create read ops for district and stock levels (simplified - real TPC-C
    // would scan order_line)
    std::string dist_key =
        "dist_" + std::to_string(w_id) + "_" + std::to_string(d_id);
    req->ops.push_back(MakeOp(TPCC_TABLE_DISTRICT, 0, dist_key, "")); // Read

    // Read some stock records (simplified)
    for (int i = 0; i < 20; i++) {
      std::string stock_key = "stock_" + std::to_string(w_id) + "_" +
                              std::to_string(RandomInt(0, 1000));
      req->ops.push_back(MakeOp(TPCC_TABLE_STOCK, 0, stock_key, "")); // Read
    }
  }

private:
  // NURand function per TPC-C spec
  int32_t NURand(int32_t A, int32_t x, int32_t y) {
    return (((RandomInt(0, A) | RandomInt(x, y + 1)) + RandomInt(0, A)) %
            (y - x + 1)) +
           x;
  }

  // Map warehouse to shard
  uint32_t WarehouseToShard(int32_t w_id) const {
    return static_cast<uint32_t>(w_id) % config_.shard_num;
  }

  // Generate TPC-C style last name
  std::string GenerateLastName(int32_t num) {
    static const char *syllables[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                      "ESE", "ANTI",  "CALLY", "ATION", "EING"};
    std::string name;
    name += syllables[num / 100];
    name += syllables[(num / 10) % 10];
    name += syllables[num % 10];
    return name;
  }

  // Key generation helpers
  std::string DistrictKey(int32_t w_id, int32_t d_id) {
    return std::to_string(w_id) + ":" + std::to_string(d_id);
  }

  std::string CustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
    return std::to_string(w_id) + ":" + std::to_string(d_id) + ":" +
           std::to_string(c_id);
  }

  std::string StockKey(int32_t w_id, int32_t i_id) {
    return std::to_string(w_id) + ":" + std::to_string(i_id);
  }

  std::string NewOrderKey(int32_t w_id, int32_t d_id) {
    return std::to_string(w_id) + ":" + std::to_string(d_id);
  }

  std::string OrderKey(int32_t w_id, int32_t d_id) {
    return std::to_string(w_id) + ":" + std::to_string(d_id);
  }

  std::string OrderLineKey(int32_t w_id, int32_t d_id) {
    return std::to_string(w_id) + ":" + std::to_string(d_id);
  }
};

} // namespace janus
