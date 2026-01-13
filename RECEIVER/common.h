#pragma once

// --- Common C++ STL Includes ---
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <stack>
#include <string_view>
#include <array>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <memory> // For std::shared_ptr

// --- Common Library Includes ---

// Arrow
#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/compute/api.h>
#include <arrow/status.h>
#include <arrow/pretty_print.h>
#include <arrow/util/logging.h>

// xxHash
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

// --- Shared Structs & Types ---

// Holds the string views for the 5 key arrays of a single transaction
struct TxKeyViews {
    std::string_view readonly_view;
    std::string_view writable_view;
    std::string_view postTokenBalances_view;
    std::string_view preTokenBalances_view;
    std::string_view accountKeys_view;
};

// Struct to hold pre/post balances.
struct BalancePair {
    std::string_view pre;
    std::string_view post; // Will be empty if not found
};

// Holds all the data needed for hot address lookups
struct HotAddressLookups {
    // This vector OWNs the strings, ensuring string_views are valid.
    std::vector<std::string> storage;
    
    // This set is for fast filtering (Task 1: Find TX)
    std::unordered_set<uint64_t> pool_hashes;
    
    // This map is for fast replacement (Task 2: Replace Owner)
    std::unordered_map<uint64_t, std::string_view> hash_to_addr_map;
};

// Each thread will have its own set of builders
struct ThreadLocalBuilders {
    arrow::StringBuilder tx_wallet_builder;
    arrow::StringBuilder signature_builder;
    arrow::StringBuilder mint_builder;
    arrow::StringBuilder pre_balance_builder;
    arrow::StringBuilder post_balance_builder;

    // Helper to append a full row
    void AppendRow(
        std::string_view wallet,
        const std::string& signature,
        std::string_view mint,
        std::string_view pre,
        std::string_view post
    );
};

// Define the map type: map<owner_wallet, map<mint, BalancePair>>
using OwnerMintMap = std::unordered_map<std::string_view, 
                                        std::unordered_map<std::string_view, BalancePair>>;