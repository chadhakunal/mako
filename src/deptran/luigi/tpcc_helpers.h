#pragma once

#include "memdb/row.h"
#include "memdb/value.h"
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace janus {

//=============================================================================
// TPC-C NURand Distribution (Non-Uniform Random)
// Spec: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) +
// x
//=============================================================================
class TPCCRandom {
public:
  std::mt19937 rng_; // Public for std::shuffle usage

private:
  // C constants for NURand (TPC-C spec 2.1.6)
  static constexpr uint32_t C_255 = 223;   // For C_LAST
  static constexpr uint32_t C_1023 = 259;  // For C_ID
  static constexpr uint32_t C_8191 = 8191; // For OL_I_ID

public:
  TPCCRandom(uint32_t seed) : rng_(seed) {}

  // Uniform random integer in [min, max]
  int32_t UniformInt(int32_t min, int32_t max) {
    std::uniform_int_distribution<int32_t> dist(min, max);
    return dist(rng_);
  }

  // Uniform random double in [min, max]
  double UniformDouble(double min, double max) {
    std::uniform_real_distribution<double> dist(min, max);
    return dist(rng_);
  }

  // NURand distribution for customer ID (A=1023, x=1, y=3000)
  int32_t NURandCID(int32_t max_cid) {
    int32_t A = 1023;
    int32_t x = 1;
    int32_t y = max_cid;
    int32_t r1 = UniformInt(0, A);
    int32_t r2 = UniformInt(x, y);
    return (((r1 | r2) + C_1023) % (y - x + 1)) + x;
  }

  // NURand distribution for customer last name (A=255, x=0, y=999)
  int32_t NURandCLast() {
    int32_t A = 255;
    int32_t x = 0;
    int32_t y = 999;
    int32_t r1 = UniformInt(0, A);
    int32_t r2 = UniformInt(x, y);
    return (((r1 | r2) + C_255) % (y - x + 1)) + x;
  }

  // NURand distribution for item ID (A=8191, x=1, y=100000)
  int32_t NURandItemID(int32_t max_item) {
    int32_t A = 8191;
    int32_t x = 1;
    int32_t y = max_item;
    int32_t r1 = UniformInt(0, A);
    int32_t r2 = UniformInt(x, y);
    return (((r1 | r2) + C_8191) % (y - x + 1)) + x;
  }

  // Random percentage check
  bool PercentageTrue(int32_t percentage) {
    return UniformInt(1, 100) <= percentage;
  }

  // Generate customer last name from number (TPC-C 4.3.2.3)
  static std::string MakeLastName(int32_t num) {
    static const char *syllables[] = {"BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                                      "ESE", "ANTI",  "CALLY", "ATION", "EING"};

    std::string name;
    name += syllables[(num / 100) % 10];
    name += syllables[(num / 10) % 10];
    name += syllables[num % 10];
    return name;
  }

  // Generate random alphanumeric string
  std::string RandomAString(int32_t min_len, int32_t max_len) {
    int32_t len = UniformInt(min_len, max_len);
    std::string result;
    result.reserve(len);
    for (int32_t i = 0; i < len; i++) {
      result += static_cast<char>('A' + UniformInt(0, 25));
    }
    return result;
  }

  // Generate random numeric string
  std::string RandomNString(int32_t min_len, int32_t max_len) {
    int32_t len = UniformInt(min_len, max_len);
    std::string result;
    result.reserve(len);
    for (int32_t i = 0; i < len; i++) {
      result += static_cast<char>('0' + UniformInt(0, 9));
    }
    return result;
  }
};

//=============================================================================
// Key Generation Helpers
//=============================================================================

inline std::string MakeWarehouseKey(int32_t w_id) {
  return "warehouse_w" + std::to_string(w_id);
}

inline std::string MakeDistrictKey(int32_t w_id, int32_t d_id) {
  return "district_w" + std::to_string(w_id) + "_d" + std::to_string(d_id);
}

inline std::string MakeCustomerKey(int32_t w_id, int32_t d_id, int32_t c_id) {
  return "customer_w" + std::to_string(w_id) + "_d" + std::to_string(d_id) +
         "_c" + std::to_string(c_id);
}

inline std::string MakeItemKey(int32_t i_id) {
  return "item_i" + std::to_string(i_id);
}

inline std::string MakeStockKey(int32_t w_id, int32_t i_id) {
  return "stock_w" + std::to_string(w_id) + "_i" + std::to_string(i_id);
}

inline std::string MakeOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  return "order_w" + std::to_string(w_id) + "_d" + std::to_string(d_id) + "_o" +
         std::to_string(o_id);
}

inline std::string MakeNewOrderKey(int32_t w_id, int32_t d_id, int32_t o_id) {
  return "neworder_w" + std::to_string(w_id) + "_d" + std::to_string(d_id) +
         "_o" + std::to_string(o_id);
}

inline std::string MakeOrderLineKey(int32_t w_id, int32_t d_id, int32_t o_id,
                                    int32_t ol_number) {
  return "orderline_w" + std::to_string(w_id) + "_d" + std::to_string(d_id) +
         "_o" + std::to_string(o_id) + "_ol" + std::to_string(ol_number);
}

inline std::string MakeHistoryKey(int32_t w_id, int32_t d_id, int32_t c_id,
                                  uint64_t timestamp) {
  return "history_w" + std::to_string(w_id) + "_d" + std::to_string(d_id) +
         "_c" + std::to_string(c_id) + "_t" + std::to_string(timestamp);
}

} // namespace janus
