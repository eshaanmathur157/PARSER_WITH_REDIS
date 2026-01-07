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

//SYNCHRONIZATION
#include <shared_mutex> 
#include <mutex>        
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
    // --- Data Storage ---
    std::vector<std::string> storage;
    std::unordered_set<uint64_t> pool_hashes;
    std::unordered_map<uint64_t, std::string_view> hash_to_addr_map;

    // --- Synchronization ---
    // 'mutable' allows us to lock it even if the struct instance is passed as 'const'
    mutable std::shared_mutex rw_mutex; 

    // --- Helper to safely add new vaults ---
    void add_new_vaults(const std::string& base_vault, const std::string& quote_vault) {
        // 1. Acquire WRITE Lock (blocks all readers)
        std::unique_lock<std::shared_mutex> lock(rw_mutex);

        // 2. Add Base Vault
        storage.push_back(base_vault);
        std::string_view base_view = storage.back();
        uint64_t base_hash = XXH3_64bits_withSeed(base_view.data(), base_view.length(), 0);
        pool_hashes.insert(base_hash);
        hash_to_addr_map[base_hash] = base_view;

        // 3. Add Quote Vault
        storage.push_back(quote_vault);
        std::string_view quote_view = storage.back();
        uint64_t quote_hash = XXH3_64bits_withSeed(quote_view.data(), quote_view.length(), 0);
        pool_hashes.insert(quote_hash);
        hash_to_addr_map[quote_hash] = quote_view;
        
        // Lock releases automatically here
        std::cout << "[Redis Update] Added Base: " << base_vault << " | Quote: " << quote_vault << std::endl;
    }
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
