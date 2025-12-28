#include "state_machine.h"
#include "__dep__.h"
#include "tpcc_constants.h"
#include "tpcc_helpers.h"

#include <algorithm>
#include <chrono>
#include <random>

namespace janus {

//=============================================================================
// LuigiMicroStateMachine Implementation
//=============================================================================

bool LuigiMicroStateMachine::Execute(
    uint32_t txn_type, const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {
  // For micro benchmark, working_set contains key-value pairs
  // Process each entry in working_set
  for (const auto &[var_id, value] : working_set) {
    // Generate key from variable ID
    std::string key = "key_" + std::to_string(var_id);

    // Determine if this is a read or write based on value
    if (value.empty()) {
      // Empty value = read operation
      std::string result;
      if (Get(key, result)) {
        if (output) {
          (*output)[key] = result;
        }
      } else {
        if (output) {
          (*output)[key] = "";
        }
      }
    } else {
      // Non-empty value = write operation
      Put(key, value);
    }
  }
  return true;
}

void LuigiMicroStateMachine::PopulateData() {
  // Populate with some initial data for micro benchmarks
  // Key format: "key_<shard>_<index>"

  const uint32_t keys_per_shard = 1000000; // 1M keys per shard

  std::default_random_engine rng(shard_id_ * 12345);
  std::uniform_int_distribution<> val_dist(0, 999999);

  for (uint32_t i = 0; i < keys_per_shard; i++) {
    std::string key =
        "key_" + std::to_string(shard_id_) + "_" + std::to_string(i);
    std::string value = std::to_string(val_dist(rng));
    Put(key, value);
  }

  Log_info("LuigiMicroStateMachine[%u]: Populated %zu keys", shard_id_,
           kv_store_.size());
}

//=============================================================================
// LuigiTPCCStateMachine Implementation
//=============================================================================

LuigiTPCCStateMachine::~LuigiTPCCStateMachine() {
  // Clean up schemas
  for (auto *schema : schemas_) {
    delete schema;
  }
  schemas_.clear();

  // TxnMgr owns tables, no need to delete them
}

void LuigiTPCCStateMachine::InitializeTables() {
  //=========================================================================
  // Create TPC-C Schemas (following Tiga's TPCCStateMachine)
  //=========================================================================

  // Warehouse table: W_ID (i32 key), W_NAME, W_YTD (double)
  {
    auto *schema = new mdb::Schema();
    schema->add_column("w_id", mdb::Value::I32, true); // primary key
    schema->add_column("w_name", mdb::Value::STR);
    schema->add_column("w_street_1", mdb::Value::STR);
    schema->add_column("w_street_2", mdb::Value::STR);
    schema->add_column("w_city", mdb::Value::STR);
    schema->add_column("w_state", mdb::Value::STR);
    schema->add_column("w_zip", mdb::Value::STR);
    schema->add_column("w_tax", mdb::Value::DOUBLE);
    schema->add_column("w_ytd", mdb::Value::DOUBLE);
    schemas_.push_back(schema);
    tbl_warehouse_ = new mdb::SortedTable("warehouse", schema);
    txn_mgr_.reg_table("warehouse", tbl_warehouse_);
  }

  // District table: D_ID + D_W_ID (composite key)
  {
    auto *schema = new mdb::Schema();
    schema->add_column("d_id", mdb::Value::I32, true);
    schema->add_column("d_w_id", mdb::Value::I32, true);
    schema->add_column("d_name", mdb::Value::STR);
    schema->add_column("d_street_1", mdb::Value::STR);
    schema->add_column("d_street_2", mdb::Value::STR);
    schema->add_column("d_city", mdb::Value::STR);
    schema->add_column("d_state", mdb::Value::STR);
    schema->add_column("d_zip", mdb::Value::STR);
    schema->add_column("d_tax", mdb::Value::DOUBLE);
    schema->add_column("d_ytd", mdb::Value::DOUBLE);
    schema->add_column("d_next_o_id", mdb::Value::I32);
    schemas_.push_back(schema);
    tbl_district_ = new mdb::SortedTable("district", schema);
    txn_mgr_.reg_table("district", tbl_district_);
  }

  // Customer table: C_ID + C_D_ID + C_W_ID (composite key)
  {
    auto *schema = new mdb::Schema();
    schema->add_column("c_id", mdb::Value::I32, true);
    schema->add_column("c_d_id", mdb::Value::I32, true);
    schema->add_column("c_w_id", mdb::Value::I32, true);
    schema->add_column("c_first", mdb::Value::STR);
    schema->add_column("c_middle", mdb::Value::STR);
    schema->add_column("c_last", mdb::Value::STR);
    schema->add_column("c_street_1", mdb::Value::STR);
    schema->add_column("c_street_2", mdb::Value::STR);
    schema->add_column("c_city", mdb::Value::STR);
    schema->add_column("c_state", mdb::Value::STR);
    schema->add_column("c_zip", mdb::Value::STR);
    schema->add_column("c_phone", mdb::Value::STR);
    schema->add_column("c_since", mdb::Value::STR);
    schema->add_column("c_credit", mdb::Value::STR);
    schema->add_column("c_credit_lim", mdb::Value::DOUBLE);
    schema->add_column("c_discount", mdb::Value::DOUBLE);
    schema->add_column("c_balance", mdb::Value::DOUBLE);
    schema->add_column("c_ytd_payment", mdb::Value::DOUBLE);
    schema->add_column("c_payment_cnt", mdb::Value::I32);
    schema->add_column("c_delivery_cnt", mdb::Value::I32);
    schema->add_column("c_data", mdb::Value::STR);
    schemas_.push_back(schema);
    tbl_customer_ = new mdb::SortedTable("customer", schema);
    txn_mgr_.reg_table("customer", tbl_customer_);
  }

  // History table (no primary key in TPC-C spec, use row_id)
  {
    auto *schema = new mdb::Schema();
    schema->add_column("h_row_id", mdb::Value::I64, true);
    schema->add_column("h_c_id", mdb::Value::I32);
    schema->add_column("h_c_d_id", mdb::Value::I32);
    schema->add_column("h_c_w_id", mdb::Value::I32);
    schema->add_column("h_d_id", mdb::Value::I32);
    schema->add_column("h_w_id", mdb::Value::I32);
    schema->add_column("h_date", mdb::Value::STR);
    schema->add_column("h_amount", mdb::Value::DOUBLE);
    schema->add_column("h_data", mdb::Value::STR);
    schemas_.push_back(schema);
    tbl_history_ = new mdb::SortedTable("history", schema);
    txn_mgr_.reg_table("history", tbl_history_);
  }

  // NewOrder table: NO_O_ID + NO_D_ID + NO_W_ID
  {
    auto *schema = new mdb::Schema();
    schema->add_column("no_o_id", mdb::Value::I32, true);
    schema->add_column("no_d_id", mdb::Value::I32, true);
    schema->add_column("no_w_id", mdb::Value::I32, true);
    schemas_.push_back(schema);
    tbl_new_order_ = new mdb::SortedTable("new_order", schema);
    txn_mgr_.reg_table("new_order", tbl_new_order_);
  }

  // Order table: O_ID + O_D_ID + O_W_ID
  {
    auto *schema = new mdb::Schema();
    schema->add_column("o_id", mdb::Value::I32, true);
    schema->add_column("o_d_id", mdb::Value::I32, true);
    schema->add_column("o_w_id", mdb::Value::I32, true);
    schema->add_column("o_c_id", mdb::Value::I32);
    schema->add_column("o_entry_d", mdb::Value::STR);
    schema->add_column("o_carrier_id", mdb::Value::I32);
    schema->add_column("o_ol_cnt", mdb::Value::I32);
    schema->add_column("o_all_local", mdb::Value::I32);
    schemas_.push_back(schema);
    tbl_order_ = new mdb::SortedTable("order", schema);
    txn_mgr_.reg_table("order", tbl_order_);
  }

  // Order C_ID Secondary Index: (O_C_ID, O_D_ID, O_W_ID) -> O_ID
  // Used by OrderStatus to find most recent order for a customer
  {
    auto *schema = new mdb::Schema();
    schema->add_column("o_c_id", mdb::Value::I32, true);
    schema->add_column("o_d_id", mdb::Value::I32, true);
    schema->add_column("o_w_id", mdb::Value::I32, true);
    schema->add_column("o_id", mdb::Value::I32); // The actual o_id value
    schemas_.push_back(schema);
    tbl_order_cid_secondary_ =
        new mdb::SortedTable("order_cid_secondary", schema);
    txn_mgr_.reg_table("order_cid_secondary", tbl_order_cid_secondary_);
  }

  // OrderLine table: OL_O_ID + OL_D_ID + OL_W_ID + OL_NUMBER
  {
    auto *schema = new mdb::Schema();
    schema->add_column("ol_o_id", mdb::Value::I32, true);
    schema->add_column("ol_d_id", mdb::Value::I32, true);
    schema->add_column("ol_w_id", mdb::Value::I32, true);
    schema->add_column("ol_number", mdb::Value::I32, true);
    schema->add_column("ol_i_id", mdb::Value::I32);
    schema->add_column("ol_supply_w_id", mdb::Value::I32);
    schema->add_column("ol_delivery_d", mdb::Value::STR);
    schema->add_column("ol_quantity", mdb::Value::I32);
    schema->add_column("ol_amount", mdb::Value::DOUBLE);
    schema->add_column("ol_dist_info", mdb::Value::STR);
    schemas_.push_back(schema);
    tbl_order_line_ = new mdb::SortedTable("order_line", schema);
    txn_mgr_.reg_table("order_line", tbl_order_line_);
  }

  // Item table: I_ID
  {
    auto *schema = new mdb::Schema();
    schema->add_column("i_id", mdb::Value::I32, true);
    schema->add_column("i_im_id", mdb::Value::I32);
    schema->add_column("i_name", mdb::Value::STR);
    schema->add_column("i_price", mdb::Value::DOUBLE);
    schema->add_column("i_data", mdb::Value::STR);
    schemas_.push_back(schema);
    tbl_item_ = new mdb::SortedTable("item", schema);
    txn_mgr_.reg_table("item", tbl_item_);
  }

  // Stock table: S_I_ID + S_W_ID
  {
    auto *schema = new mdb::Schema();
    schema->add_column("s_i_id", mdb::Value::I32, true);
    schema->add_column("s_w_id", mdb::Value::I32, true);
    schema->add_column("s_quantity", mdb::Value::I32);
    schema->add_column("s_dist_01", mdb::Value::STR);
    schema->add_column("s_dist_02", mdb::Value::STR);
    schema->add_column("s_dist_03", mdb::Value::STR);
    schema->add_column("s_dist_04", mdb::Value::STR);
    schema->add_column("s_dist_05", mdb::Value::STR);
    schema->add_column("s_dist_06", mdb::Value::STR);
    schema->add_column("s_dist_07", mdb::Value::STR);
    schema->add_column("s_dist_08", mdb::Value::STR);
    schema->add_column("s_dist_09", mdb::Value::STR);
    schema->add_column("s_dist_10", mdb::Value::STR);
    schema->add_column("s_ytd", mdb::Value::I32);
    schema->add_column("s_order_cnt", mdb::Value::I32);
    schema->add_column("s_remote_cnt", mdb::Value::I32);
    schema->add_column("s_data", mdb::Value::STR);
    schemas_.push_back(schema);
    tbl_stock_ = new mdb::SortedTable("stock", schema);
    txn_mgr_.reg_table("stock", tbl_stock_);
  }

  Log_info("LuigiTPCCStateMachine[%u]: Tables initialized", shard_id_);
}

void LuigiTPCCStateMachine::PopulateData() {
  // Note: This is simplified data loading
  // Production code would need proper TPC-C data population

  std::default_random_engine rng(shard_id_ * 54321);

  // Populate warehouses (each shard owns some warehouses)
  auto *txn = txn_mgr_.start(0);

  for (uint32_t w = 0; w < num_warehouses_; w++) {
    if (KeyToShard("warehouse_" + std::to_string(w)) != shard_id_) {
      continue; // This warehouse belongs to another shard
    }

    // Create warehouse row
    std::vector<mdb::Value> row_data = {
        mdb::Value((i32)w),                           // w_id
        mdb::Value("Warehouse_" + std::to_string(w)), // w_name
        mdb::Value("Street1"),                        // w_street_1
        mdb::Value("Street2"),                        // w_street_2
        mdb::Value("City"),                           // w_city
        mdb::Value("ST"),                             // w_state
        mdb::Value("12345"),                          // w_zip
        mdb::Value(0.0875),                           // w_tax
        mdb::Value(300000.0)                          // w_ytd
    };
    auto *row = mdb::Row::create(tbl_warehouse_->schema(), row_data);
    txn->insert_row(tbl_warehouse_, row);

    // Create districts for this warehouse
    for (uint32_t d = 0; d < num_districts_per_wh_; d++) {
      std::vector<mdb::Value> dist_data = {
          mdb::Value((i32)d),                          // d_id
          mdb::Value((i32)w),                          // d_w_id
          mdb::Value("District_" + std::to_string(d)), // d_name
          mdb::Value("DStreet1"),
          mdb::Value("DStreet2"),
          mdb::Value("DCity"),
          mdb::Value("ST"),
          mdb::Value("12345"),
          mdb::Value(0.0875),   // d_tax
          mdb::Value(30000.0),  // d_ytd
          mdb::Value((i32)3001) // d_next_o_id
      };
      auto *drow = mdb::Row::create(tbl_district_->schema(), dist_data);
      txn->insert_row(tbl_district_, drow);
    }
  }

  // Populate items (same on all shards)
  for (uint32_t i = 0; i < num_items_; i++) {
    std::vector<mdb::Value> item_data = {
        mdb::Value((i32)i),                      // i_id
        mdb::Value((i32)(i % 10000)),            // i_im_id
        mdb::Value("Item_" + std::to_string(i)), // i_name
        mdb::Value(1.0 + (i % 100)),             // i_price
        mdb::Value("ItemData")                   // i_data
    };
    auto *row = mdb::Row::create(tbl_item_->schema(), item_data);
    txn->insert_row(tbl_item_, row);
  }

  delete txn;

  Log_info("LuigiTPCCStateMachine[%u]: Data populated", shard_id_);
}

bool LuigiTPCCStateMachine::Execute(
    uint32_t txn_type, const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  // Dispatch to appropriate TPC-C transaction handler
  switch (txn_type) {
  case LUIGI_TXN_NEW_ORDER:
    return ExecuteNewOrder(working_set, output, txn_id);
  case LUIGI_TXN_PAYMENT:
    return ExecutePayment(working_set, output, txn_id);
  case LUIGI_TXN_ORDER_STATUS:
    return ExecuteOrderStatus(working_set, output, txn_id);
  case LUIGI_TXN_DELIVERY:
    return ExecuteDelivery(working_set, output, txn_id);
  case LUIGI_TXN_STOCK_LEVEL:
    return ExecuteStockLevel(working_set, output, txn_id);
  default:
    Log_warn("Unknown TPC-C transaction type: %u", txn_type);
    return false;
  }
}

void LuigiTPCCStateMachine::MapOpsToShards(
    uint32_t txn_type, const std::vector<LuigiOp> &ops,
    std::map<uint32_t, std::vector<LuigiOp>> *shard_ops) {
  // TPC-C sharding is by warehouse
  // Extract w_id from op keys and route accordingly
  for (const auto &op : ops) {
    uint32_t target_shard = KeyToShard(op.key);
    (*shard_ops)[target_shard].push_back(op);
  }
}

uint32_t LuigiTPCCStateMachine::KeyToShard(const std::string &key) const {
  // TPC-C sharding: extract warehouse ID from key
  // Key format: "table_w<w_id>_..."

  // Simple approach: find "w" followed by digits
  size_t w_pos = key.find("_w");
  if (w_pos != std::string::npos) {
    w_pos += 2; // Skip "_w"
    size_t end = w_pos;
    while (end < key.length() && isdigit(key[end])) {
      end++;
    }
    if (end > w_pos) {
      int w_id = std::stoi(key.substr(w_pos, end - w_pos));
      return w_id % shard_num_;
    }
  }

  // Fallback to parent implementation
  return LuigiStateMachine::KeyToShard(key);
}

//=============================================================================
// TPC-C Transaction Implementations (Simplified)
//=============================================================================

bool LuigiTPCCStateMachine::ExecuteNewOrder(
    const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  // Extract parameters from working_set
  int32_t w_id = std::stoi(working_set.at(TPCC_VAR_W_ID));
  int32_t d_id = std::stoi(working_set.at(TPCC_VAR_D_ID));
  int32_t c_id = std::stoi(working_set.at(TPCC_VAR_C_ID));
  int32_t ol_cnt = std::stoi(working_set.at(TPCC_VAR_OL_CNT));

  auto *txn = txn_mgr_.start(txn_id);

  // 1. Read warehouse
  mdb::ResultSet w_rs = txn->query(tbl_warehouse_, mdb::Value(w_id));
  mdb::Row *w_row = w_rs.has_next() ? w_rs.next() : nullptr;
  if (!w_row) {
    delete txn;
    return false;
  }

  mdb::Value w_tax_val;
  txn->read_column(w_row, tbl_warehouse_->schema()->get_column_id("w_tax"),
                   &w_tax_val);
  double w_tax = w_tax_val.get_double();

  // 2. Read and update district
  std::vector<mdb::Value> d_key_vals = {mdb::Value(d_id), mdb::Value(w_id)};
  mdb::MultiBlob d_mb(2);
  d_mb[0] = d_key_vals[0].get_blob();
  d_mb[1] = d_key_vals[1].get_blob();
  mdb::ResultSet d_rs = txn->query(tbl_district_, d_mb);
  mdb::Row *d_row = d_rs.has_next() ? d_rs.next() : nullptr;
  if (!d_row) {
    delete txn;
    return false;
  }

  mdb::Value d_tax_val, d_next_o_id_val;
  txn->read_column(d_row, tbl_district_->schema()->get_column_id("d_tax"),
                   &d_tax_val);
  txn->read_column(d_row, tbl_district_->schema()->get_column_id("d_next_o_id"),
                   &d_next_o_id_val);
  double d_tax = d_tax_val.get_double();
  int32_t d_next_o_id = d_next_o_id_val.get_i32();

  // Update d_next_o_id
  txn->write_column(d_row,
                    tbl_district_->schema()->get_column_id("d_next_o_id"),
                    mdb::Value(d_next_o_id + 1));

  // 3. Read customer
  std::vector<mdb::Value> c_key_vals = {mdb::Value(c_id), mdb::Value(d_id),
                                        mdb::Value(w_id)};
  mdb::MultiBlob c_mb(3);
  c_mb[0] = c_key_vals[0].get_blob();
  c_mb[1] = c_key_vals[1].get_blob();
  c_mb[2] = c_key_vals[2].get_blob();
  mdb::ResultSet c_rs = txn->query(tbl_customer_, c_mb);
  mdb::Row *c_row = c_rs.has_next() ? c_rs.next() : nullptr;
  if (!c_row) {
    delete txn;
    return false;
  }

  mdb::Value c_discount_val;
  txn->read_column(c_row, tbl_customer_->schema()->get_column_id("c_discount"),
                   &c_discount_val);
  double c_discount = c_discount_val.get_double();

  // 4. Process each item
  bool all_local = true;
  double total_amount = 0.0;

  for (int32_t i = 0; i < ol_cnt; i++) {
    int32_t ol_i_id = std::stoi(working_set.at(TPCC_VAR_I_ID(i)));
    int32_t ol_supply_w_id = std::stoi(working_set.at(TPCC_VAR_S_W_ID(i)));
    int32_t ol_quantity = std::stoi(working_set.at(TPCC_VAR_OL_QUANTITY(i)));

    if (ol_supply_w_id != w_id) {
      all_local = false;
    }

    // Read item
    mdb::ResultSet i_rs = txn->query(tbl_item_, mdb::Value(ol_i_id));
    mdb::Row *i_row = i_rs.has_next() ? i_rs.next() : nullptr;
    if (!i_row) {
      // TPC-C spec: 1% rollback for invalid item
      // Luigi's single-phase execution doesn't support rollback after
      // coordination For research purposes, treat as successful no-op (skip
      // this item)
      Log_warn("Invalid item %d in NewOrder - skipping (Luigi single-phase)",
               ol_i_id);
      continue; // Skip this item and continue with next
    }

    mdb::Value i_price_val;
    txn->read_column(i_row, tbl_item_->schema()->get_column_id("i_price"),
                     &i_price_val);
    double i_price = i_price_val.get_double();

    // Read and update stock
    std::vector<mdb::Value> s_key_vals = {mdb::Value(ol_i_id),
                                          mdb::Value(ol_supply_w_id)};
    mdb::MultiBlob s_mb(2);
    s_mb[0] = s_key_vals[0].get_blob();
    s_mb[1] = s_key_vals[1].get_blob();
    mdb::ResultSet s_rs = txn->query(tbl_stock_, s_mb);
    mdb::Row *s_row = s_rs.has_next() ? s_rs.next() : nullptr;
    if (!s_row) {
      delete txn;
      return false;
    }

    mdb::Value s_quantity_val, s_ytd_val, s_order_cnt_val, s_remote_cnt_val;
    txn->read_column(s_row, tbl_stock_->schema()->get_column_id("s_quantity"),
                     &s_quantity_val);
    txn->read_column(s_row, tbl_stock_->schema()->get_column_id("s_ytd"),
                     &s_ytd_val);
    txn->read_column(s_row, tbl_stock_->schema()->get_column_id("s_order_cnt"),
                     &s_order_cnt_val);
    txn->read_column(s_row, tbl_stock_->schema()->get_column_id("s_remote_cnt"),
                     &s_remote_cnt_val);

    int32_t s_quantity = s_quantity_val.get_i32();
    int32_t s_ytd = s_ytd_val.get_i32();
    int32_t s_order_cnt = s_order_cnt_val.get_i32();
    int32_t s_remote_cnt = s_remote_cnt_val.get_i32();

    // Update stock quantity (TPC-C wraparound logic)
    if (s_quantity >= ol_quantity + 10) {
      s_quantity -= ol_quantity;
    } else {
      s_quantity = s_quantity - ol_quantity + 91;
    }

    txn->write_column(s_row, tbl_stock_->schema()->get_column_id("s_quantity"),
                      mdb::Value(s_quantity));
    txn->write_column(s_row, tbl_stock_->schema()->get_column_id("s_ytd"),
                      mdb::Value(s_ytd + ol_quantity));
    txn->write_column(s_row, tbl_stock_->schema()->get_column_id("s_order_cnt"),
                      mdb::Value(s_order_cnt + 1));

    if (ol_supply_w_id != w_id) {
      txn->write_column(s_row,
                        tbl_stock_->schema()->get_column_id("s_remote_cnt"),
                        mdb::Value(s_remote_cnt + 1));
    }

    // Calculate amount
    double ol_amount = ol_quantity * i_price;
    total_amount += ol_amount;

    // Insert order_line
    std::vector<mdb::Value> ol_data = {
        mdb::Value(d_next_o_id), mdb::Value(d_id),
        mdb::Value(w_id),        mdb::Value(i + 1),
        mdb::Value(ol_i_id),     mdb::Value(ol_supply_w_id),
        mdb::Value(ol_quantity), mdb::Value(ol_amount),
        mdb::Value("dist_info")};
    auto *ol_row = mdb::Row::create(tbl_order_line_->schema(), ol_data);
    txn->insert_row(tbl_order_line_, ol_row);
  }

  // Apply discount and tax
  total_amount *= (1 - c_discount) * (1 + w_tax + d_tax);

  // 5. Insert order
  std::vector<mdb::Value> o_data = {
      mdb::Value(d_next_o_id),
      mdb::Value(d_id),
      mdb::Value(w_id),
      mdb::Value(c_id),
      mdb::Value((int32_t)std::time(nullptr)),
      mdb::Value((int32_t)0), // o_carrier_id (null)
      mdb::Value(ol_cnt),
      mdb::Value(all_local ? 1 : 0)};
  auto *o_row = mdb::Row::create(tbl_order_->schema(), o_data);
  txn->insert_row(tbl_order_, o_row);

  // Insert into secondary index for OrderStatus lookup
  std::vector<mdb::Value> idx_data = {mdb::Value(c_id), mdb::Value(d_id),
                                      mdb::Value(w_id),
                                      mdb::Value(d_next_o_id)};
  auto *idx_row =
      mdb::Row::create(tbl_order_cid_secondary_->schema(), idx_data);
  txn->insert_row(tbl_order_cid_secondary_, idx_row);

  // 6. Insert new_order
  std::vector<mdb::Value> no_data = {mdb::Value(d_next_o_id), mdb::Value(d_id),
                                     mdb::Value(w_id)};
  auto *no_row = mdb::Row::create(tbl_new_order_->schema(), no_data);
  txn->insert_row(tbl_new_order_, no_row);

  if (output) {
    (*output)["total"] = std::to_string(total_amount);
  }

  delete txn;
  return true;
}

bool LuigiTPCCStateMachine::ExecutePayment(
    const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  // Extract parameters
  int32_t w_id = std::stoi(working_set.at(TPCC_VAR_W_ID));
  int32_t d_id = std::stoi(working_set.at(TPCC_VAR_D_ID));
  int32_t c_id = std::stoi(working_set.at(TPCC_VAR_C_ID));
  double h_amount = std::stod(working_set.at(TPCC_VAR_H_AMOUNT));

  auto *txn = txn_mgr_.start(txn_id);

  // 1. Update warehouse YTD
  mdb::ResultSet w_rs = txn->query(tbl_warehouse_, mdb::Value(w_id));
  mdb::Row *w_row = w_rs.has_next() ? w_rs.next() : nullptr;
  if (!w_row) {
    delete txn;
    return false;
  }

  mdb::Value w_ytd_val;
  txn->read_column(w_row, tbl_warehouse_->schema()->get_column_id("w_ytd"),
                   &w_ytd_val);
  double w_ytd = w_ytd_val.get_double();
  txn->write_column(w_row, tbl_warehouse_->schema()->get_column_id("w_ytd"),
                    mdb::Value(w_ytd + h_amount));

  // 2. Update district YTD
  std::vector<mdb::Value> d_key_vals = {mdb::Value(d_id), mdb::Value(w_id)};
  mdb::MultiBlob d_mb(2);
  d_mb[0] = d_key_vals[0].get_blob();
  d_mb[1] = d_key_vals[1].get_blob();
  mdb::ResultSet d_rs = txn->query(tbl_district_, d_mb);
  mdb::Row *d_row = d_rs.has_next() ? d_rs.next() : nullptr;
  if (!d_row) {
    delete txn;
    return false;
  }

  mdb::Value d_ytd_val;
  txn->read_column(d_row, tbl_district_->schema()->get_column_id("d_ytd"),
                   &d_ytd_val);
  double d_ytd = d_ytd_val.get_double();
  txn->write_column(d_row, tbl_district_->schema()->get_column_id("d_ytd"),
                    mdb::Value(d_ytd + h_amount));

  // 3. Update customer
  std::vector<mdb::Value> c_key_vals = {mdb::Value(c_id), mdb::Value(d_id),
                                        mdb::Value(w_id)};
  mdb::MultiBlob c_mb(3);
  c_mb[0] = c_key_vals[0].get_blob();
  c_mb[1] = c_key_vals[1].get_blob();
  c_mb[2] = c_key_vals[2].get_blob();
  mdb::ResultSet c_rs = txn->query(tbl_customer_, c_mb);
  mdb::Row *c_row = c_rs.has_next() ? c_rs.next() : nullptr;
  if (!c_row) {
    delete txn;
    return false;
  }

  mdb::Value c_balance_val, c_ytd_payment_val, c_payment_cnt_val, c_credit_val,
      c_data_val;
  txn->read_column(c_row, tbl_customer_->schema()->get_column_id("c_balance"),
                   &c_balance_val);
  txn->read_column(c_row,
                   tbl_customer_->schema()->get_column_id("c_ytd_payment"),
                   &c_ytd_payment_val);
  txn->read_column(c_row,
                   tbl_customer_->schema()->get_column_id("c_payment_cnt"),
                   &c_payment_cnt_val);
  txn->read_column(c_row, tbl_customer_->schema()->get_column_id("c_credit"),
                   &c_credit_val);

  double c_balance = c_balance_val.get_double();
  double c_ytd_payment = c_ytd_payment_val.get_double();
  int32_t c_payment_cnt = c_payment_cnt_val.get_i32();
  std::string c_credit = c_credit_val.get_str();

  txn->write_column(c_row, tbl_customer_->schema()->get_column_id("c_balance"),
                    mdb::Value(c_balance - h_amount));
  txn->write_column(c_row,
                    tbl_customer_->schema()->get_column_id("c_ytd_payment"),
                    mdb::Value(c_ytd_payment + h_amount));
  txn->write_column(c_row,
                    tbl_customer_->schema()->get_column_id("c_payment_cnt"),
                    mdb::Value(c_payment_cnt + 1));

  // Bad credit handling
  if (c_credit == "BC") {
    txn->read_column(c_row, tbl_customer_->schema()->get_column_id("c_data"),
                     &c_data_val);
    std::string c_data = c_data_val.get_str();
    std::string new_data = std::to_string(c_id) + "," + std::to_string(d_id) +
                           "," + std::to_string(w_id) + "," +
                           std::to_string(h_amount) + "," +
                           c_data.substr(0, 450);
    txn->write_column(c_row, tbl_customer_->schema()->get_column_id("c_data"),
                      mdb::Value(new_data));
  }

  // 4. Insert history record
  std::vector<mdb::Value> h_data = {
      mdb::Value(c_id),     mdb::Value(d_id),
      mdb::Value(w_id),     mdb::Value(d_id),
      mdb::Value(w_id),     mdb::Value((int32_t)std::time(nullptr)),
      mdb::Value(h_amount), mdb::Value("payment_data")};
  auto *h_row = mdb::Row::create(tbl_history_->schema(), h_data);
  txn->insert_row(tbl_history_, h_row);

  if (output) {
    (*output)["c_balance"] = std::to_string(c_balance - h_amount);
  }

  delete txn;
  return true;
}

bool LuigiTPCCStateMachine::ExecuteOrderStatus(
    const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  int32_t w_id = std::stoi(working_set.at(TPCC_VAR_W_ID));
  int32_t d_id = std::stoi(working_set.at(TPCC_VAR_D_ID));
  int32_t c_id = std::stoi(working_set.at(TPCC_VAR_C_ID));

  auto *txn = txn_mgr_.start(txn_id);

  // Read customer
  std::vector<mdb::Value> c_key_vals = {mdb::Value(c_id), mdb::Value(d_id),
                                        mdb::Value(w_id)};
  mdb::MultiBlob c_mb(3);
  c_mb[0] = c_key_vals[0].get_blob();
  c_mb[1] = c_key_vals[1].get_blob();
  c_mb[2] = c_key_vals[2].get_blob();
  mdb::ResultSet c_rs = txn->query(tbl_customer_, c_mb);
  mdb::Row *c_row = c_rs.has_next() ? c_rs.next() : nullptr;
  if (!c_row) {
    delete txn;
    return false;
  }

  // Find most recent order for this customer using secondary index
  // Query range: (c_id, d_id, w_id, MIN) to (c_id, d_id, w_id, MAX)
  std::vector<mdb::Value> idx_key_vals = {mdb::Value(c_id), mdb::Value(d_id),
                                          mdb::Value(w_id)};
  mdb::MultiBlob idx_mbl(4), idx_mbh(4);
  idx_mbl[0] = idx_key_vals[0].get_blob();
  idx_mbl[1] = idx_key_vals[1].get_blob();
  idx_mbl[2] = idx_key_vals[2].get_blob();
  idx_mbh[0] = idx_key_vals[0].get_blob();
  idx_mbh[1] = idx_key_vals[1].get_blob();
  idx_mbh[2] = idx_key_vals[2].get_blob();

  mdb::Value o_id_low(std::numeric_limits<int32_t>::min());
  mdb::Value o_id_high(std::numeric_limits<int32_t>::max());
  idx_mbl[3] = o_id_low.get_blob();
  idx_mbh[3] = o_id_high.get_blob();

  mdb::ResultSet idx_rs =
      txn->query_in(tbl_order_cid_secondary_, idx_mbl, idx_mbh, mdb::ORD_DESC);
  mdb::Row *idx_row = idx_rs.has_next() ? idx_rs.next() : nullptr;

  if (!idx_row) {
    // No orders for this customer
    delete txn;
    return false;
  }

  // Get the o_id from secondary index
  mdb::Value o_id_val;
  txn->read_column(idx_row,
                   tbl_order_cid_secondary_->schema()->get_column_id("o_id"),
                   &o_id_val);
  int32_t o_id = o_id_val.get_i32();

  // Read order
  std::vector<mdb::Value> o_key_vals = {mdb::Value(o_id), mdb::Value(d_id),
                                        mdb::Value(w_id)};
  mdb::MultiBlob o_mb(3);
  o_mb[0] = o_key_vals[0].get_blob();
  o_mb[1] = o_key_vals[1].get_blob();
  o_mb[2] = o_key_vals[2].get_blob();
  mdb::ResultSet o_rs = txn->query(tbl_order_, o_mb);
  mdb::Row *o_row = o_rs.has_next() ? o_rs.next() : nullptr;

  if (output && o_row) {
    mdb::Value o_carrier_id_val;
    txn->read_column(o_row, tbl_order_->schema()->get_column_id("o_carrier_id"),
                     &o_carrier_id_val);
    (*output)["o_carrier_id"] = std::to_string(o_carrier_id_val.get_i32());
    (*output)["o_id"] = std::to_string(o_id);
  }

  delete txn;
  return true;
}

bool LuigiTPCCStateMachine::ExecuteDelivery(
    const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  int32_t w_id = std::stoi(working_set.at(TPCC_VAR_W_ID));
  int32_t o_carrier_id = std::stoi(working_set.at(TPCC_VAR_O_CARRIER_ID));

  auto *txn = txn_mgr_.start(txn_id);

  // Process all 10 districts
  for (int32_t d_id = 1; d_id <= 10; d_id++) {
    // Find MIN(no_o_id) for this district using query_in
    std::vector<mdb::Value> no_key_vals = {mdb::Value(d_id), mdb::Value(w_id)};
    mdb::MultiBlob no_mbl(3), no_mbh(3);
    no_mbl[0] = no_key_vals[0].get_blob();
    no_mbl[1] = no_key_vals[1].get_blob();
    no_mbh[0] = no_key_vals[0].get_blob();
    no_mbh[1] = no_key_vals[1].get_blob();

    mdb::Value no_o_id_low(std::numeric_limits<int32_t>::min());
    mdb::Value no_o_id_high(std::numeric_limits<int32_t>::max());
    no_mbl[2] = no_o_id_low.get_blob();
    no_mbh[2] = no_o_id_high.get_blob();

    mdb::ResultSet no_rs =
        txn->query_in(tbl_new_order_, no_mbl, no_mbh, mdb::ORD_ASC);
    mdb::Row *no_row = no_rs.has_next() ? no_rs.next() : nullptr;

    if (!no_row) {
      // No undelivered orders for this district
      continue;
    }

    // Get the o_id from new_order row
    mdb::Value no_o_id_val;
    txn->read_column(no_row, tbl_new_order_->schema()->get_column_id("no_o_id"),
                     &no_o_id_val);
    int32_t o_id = no_o_id_val.get_i32();

    // Delete from new_order
    txn->remove_row(tbl_new_order_, no_row);

    // Update order
    std::vector<mdb::Value> o_key_vals = {mdb::Value(o_id), mdb::Value(d_id),
                                          mdb::Value(w_id)};
    mdb::MultiBlob o_mb(3);
    o_mb[0] = o_key_vals[0].get_blob();
    o_mb[1] = o_key_vals[1].get_blob();
    o_mb[2] = o_key_vals[2].get_blob();
    mdb::ResultSet o_rs = txn->query(tbl_order_, o_mb);
    mdb::Row *o_row = o_rs.has_next() ? o_rs.next() : nullptr;
    if (o_row) {
      txn->write_column(o_row,
                        tbl_order_->schema()->get_column_id("o_carrier_id"),
                        mdb::Value(o_carrier_id));
    }
  }

  delete txn;
  return true;
}

bool LuigiTPCCStateMachine::ExecuteStockLevel(
    const std::map<int32_t, std::string> &working_set,
    std::map<std::string, std::string> *output, uint64_t txn_id) {

  int32_t w_id = std::stoi(working_set.at(TPCC_VAR_W_ID));
  int32_t d_id = std::stoi(working_set.at(TPCC_VAR_D_ID));
  int32_t threshold = std::stoi(working_set.at(TPCC_VAR_THRESHOLD));

  auto *txn = txn_mgr_.start(txn_id);

  // Read district to get d_next_o_id
  std::vector<mdb::Value> d_key_vals = {mdb::Value(d_id), mdb::Value(w_id)};
  mdb::MultiBlob d_mb(2);
  d_mb[0] = d_key_vals[0].get_blob();
  d_mb[1] = d_key_vals[1].get_blob();
  mdb::ResultSet d_rs = txn->query(tbl_district_, d_mb);
  mdb::Row *d_row = d_rs.has_next() ? d_rs.next() : nullptr;
  if (!d_row) {
    delete txn;
    return false;
  }

  mdb::Value d_next_o_id_val;
  txn->read_column(d_row, tbl_district_->schema()->get_column_id("d_next_o_id"),
                   &d_next_o_id_val);
  int32_t d_next_o_id = d_next_o_id_val.get_i32();

  // Count distinct items with stock < threshold in last 20 orders
  // Use range scan for order lines: [d_next_o_id-20, d_next_o_id)
  std::set<int32_t> low_stock_items;

  std::vector<mdb::Value> ol_key_vals = {mdb::Value(d_id), mdb::Value(w_id)};
  mdb::MultiBlob ol_mbl(4), ol_mbh(4);
  ol_mbl[0] = ol_key_vals[0].get_blob();
  ol_mbl[1] = ol_key_vals[1].get_blob();
  ol_mbh[0] = ol_key_vals[0].get_blob();
  ol_mbh[1] = ol_key_vals[1].get_blob();

  mdb::Value o_id_low(d_next_o_id - 20);
  mdb::Value o_id_high(d_next_o_id - 1);
  ol_mbl[2] = o_id_low.get_blob();
  ol_mbh[2] = o_id_high.get_blob();

  mdb::Value ol_number_low(std::numeric_limits<int32_t>::min());
  mdb::Value ol_number_high(std::numeric_limits<int32_t>::max());
  ol_mbl[3] = ol_number_low.get_blob();
  ol_mbh[3] = ol_number_high.get_blob();

  mdb::ResultSet ol_rs =
      txn->query_in(tbl_order_line_, ol_mbl, ol_mbh, mdb::ORD_ASC);

  while (ol_rs.has_next()) {
    mdb::Row *ol_row = ol_rs.next();

    mdb::Value i_id_val;
    txn->read_column(
        ol_row, tbl_order_line_->schema()->get_column_id("ol_i_id"), &i_id_val);
    int32_t i_id = i_id_val.get_i32();

    // Check stock
    std::vector<mdb::Value> s_key_vals = {mdb::Value(i_id), mdb::Value(w_id)};
    mdb::MultiBlob s_mb(2);
    s_mb[0] = s_key_vals[0].get_blob();
    s_mb[1] = s_key_vals[1].get_blob();
    mdb::ResultSet s_rs = txn->query(tbl_stock_, s_mb);
    mdb::Row *s_row = s_rs.has_next() ? s_rs.next() : nullptr;
    if (!s_row)
      continue;

    mdb::Value s_quantity_val;
    txn->read_column(s_row, tbl_stock_->schema()->get_column_id("s_quantity"),
                     &s_quantity_val);
    if (s_quantity_val.get_i32() < threshold) {
      low_stock_items.insert(i_id);
    }
  }

  if (output) {
    (*output)["low_stock"] = std::to_string(low_stock_items.size());
  }

  delete txn;
  return true;
}

} // namespace janus
