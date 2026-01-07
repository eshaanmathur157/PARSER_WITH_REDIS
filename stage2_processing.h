#ifndef STAGE2_PROCESSING_H
#define STAGE2_PROCESSING_H

#include <vector>
#include <string>
#include <string_view>
#include <atomic>
#include <stack>
#include <iostream>
#include <algorithm>
#include <thread>
#include <unordered_map>
#include <unordered_set>

// Your dependencies
#include "common.h" 
#include "xxhash.h" 

// --- Forward Decls ---
// (If ThreadLocalBuilders or TxKeyViews are defined in common.h, you are good. 
//  If not, ensure their structs are visible here).

class SkipMapBuilder {
private:
    std::vector<size_t> m_skipMap;
    const std::vector<uint64_t>& m_list;
    const char* m_jsonContent;
public:
    SkipMapBuilder(const std::vector<uint64_t>& list, const char* json_data);
    void buildSkipMap();
    const std::vector<size_t>& getSkipMap() const;
};

// --- Function Declarations ---

std::vector<TxKeyViews> find_transaction_views(
    const std::vector<uint64_t>& index_list,
    const std::vector<size_t>& skipMap,
    const char* json_buffer
);

// --- MODIFIED SIGNATURE TO MATCH CPP ---
// Returns void (sends via network), takes flight_server_uri
void process_transactions_parallel(
    const std::vector<TxKeyViews>& transaction_views,
    const HotAddressLookups& hot_addresses,
    const std::string& global_start_time_str,
    const std::string& flight_server_uri, 
    std::atomic<size_t>& pool_tx_counter
);

#endif // STAGE2_PROCESSING_H
