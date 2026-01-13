#include "stage2_processing.h"

// --- ADDED: Arrow Flight Headers ---
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/client.h>

// --- Existing SkipMapBuilder Implementation (Unchanged) ---
SkipMapBuilder::SkipMapBuilder(const std::vector<uint64_t>& list, const char* json_data)
    : m_list(list), m_jsonContent(json_data)
{
    std::cout << "Building skip-map (on small index)..." << std::endl;
    buildSkipMap();
}

void SkipMapBuilder::buildSkipMap() {
    m_skipMap.resize(m_list.size(), 0);
    std::stack<size_t> stack;
    for (size_t i = 0; i < m_list.size(); ++i) {
        char ch = m_jsonContent[m_list[i]];
        if (ch == '[') {
            stack.push(i);
        } else if (ch == ']') {
            if (!stack.empty()) {
                size_t open_index = stack.top();
                stack.pop();
                m_skipMap[open_index] = i;
            }
        }
    }
}

const std::vector<size_t>& SkipMapBuilder::getSkipMap() const {
    return m_skipMap;
}

// --- ThreadLocalBuilders Method Definition (Unchanged) ---
void ThreadLocalBuilders::AppendRow(
    std::string_view wallet,
    const std::string& signature,
    std::string_view mint,
    std::string_view pre,
    std::string_view post
) {
    (void)tx_wallet_builder.Append(wallet);
    (void)signature_builder.Append(signature);
    (void)mint_builder.Append(mint);
    if (pre.empty()) {
        (void)pre_balance_builder.AppendNull();
    } else {
        (void)pre_balance_builder.Append(pre);
    }
    if (post.empty()) {
        (void)post_balance_builder.AppendNull();
    } else {
        (void)post_balance_builder.Append(post);
    }
}

// --- Helper Function Definitions (Static - Unchanged) ---
static void parse_string_array(std::string_view view, std::vector<std::string_view>& out_strings) {
    out_strings.clear();
    size_t pos = 0;
    const size_t len = view.length();

    while (pos < len) {
        size_t start = view.find('"', pos);
        if (start == std::string_view::npos) break;

        size_t end = view.find('"', start + 1);
        if (end == std::string_view::npos) break;

        out_strings.push_back(view.substr(start + 1, end - start - 1));
        pos = end + 1;
    }
}

static bool parse_token_balance_object_with_owner(
    std::string_view view,
    size_t& pos,
    int& out_account_index,
    std::string_view& out_mint,
    std::string_view& out_owner,
    std::string_view& out_balance
) {
    const std::string_view index_key = "\"accountIndex\":";
    const std::string_view mint_key = "\"mint\":\"";
    const std::string_view owner_key = "\"owner\":\"";
    const std::string_view balance_key = "\"uiAmountString\":\"";
    size_t index_start = view.find(index_key, pos);
    if (index_start == std::string_view::npos) return false;
    size_t index_val_start = index_start + index_key.length();
    size_t index_val_end = view.find_first_of(",}", index_val_start);
    if (index_val_end == std::string_view::npos) return false;
    out_account_index = 0;
    for (size_t i = index_val_start; i < index_val_end; ++i) {
        char c = view[i];
        if (c >= '0' && c <= '9') {
            out_account_index = out_account_index * 10 + (c - '0');
        }
    }
    size_t mint_start = view.find(mint_key, index_val_end);
    if (mint_start == std::string_view::npos) {
        pos = index_val_end + 1;
        return true;
    }
    size_t prev_index_check = view.rfind(index_key, mint_start);
    if (prev_index_check != index_start) {
        pos = index_val_end + 1;
        return true;
    }
    size_t mint_val_start = mint_start + mint_key.length();
    size_t mint_val_end = view.find('"', mint_val_start);
    if (mint_val_end == std::string_view::npos) return false;
    out_mint = view.substr(mint_val_start, mint_val_end - mint_val_start);
    size_t owner_start = view.find(owner_key, mint_val_end);
    if (owner_start == std::string_view::npos) {
         pos = mint_val_end + 1;
         return true;
    }
    size_t prev_mint_check = view.rfind(mint_key, owner_start);
    if (prev_mint_check != mint_start) {
        pos = mint_val_end + 1;
        return true;
    }
    size_t owner_val_start = owner_start + owner_key.length();
    size_t owner_val_end = view.find('"', owner_val_start);
    if (owner_val_end == std::string_view::npos) return false;
    out_owner = view.substr(owner_val_start, owner_val_end - owner_val_start);
    size_t balance_start = view.find(balance_key, owner_val_end);
    if (balance_start == std::string_view::npos) {
        out_balance = {};
        pos = owner_val_end + 1;
        return true;
    }
    size_t prev_owner_check = view.rfind(owner_key, balance_start);
    if (prev_owner_check != owner_start) {
        out_balance = {};
        pos = owner_val_end + 1;
        return true;
    }
    size_t balance_val_start = balance_start + balance_key.length();
    size_t balance_val_end = view.find('"', balance_val_start);
    if (balance_val_end == std::string_view::npos) return false;
    out_balance = view.substr(balance_val_start, balance_val_end - balance_val_start);
    pos = balance_val_end + 1;
    return true;
}

static void parse_token_balance_array(
    std::string_view view,
    OwnerMintMap& tx_map,
    bool is_pre_balance,
    const std::unordered_map<int, std::string_view>& index_to_hot_addr_map
) {
    size_t pos = 0;
    int account_index;
    std::string_view mint, owner, balance;
    while (parse_token_balance_object_with_owner(view, pos, account_index, mint, owner, balance)) {
        std::string_view wallet_key;
        auto it = index_to_hot_addr_map.find(account_index);

        if (it != index_to_hot_addr_map.end()) {
            wallet_key = it->second;
        } else {
            wallet_key = owner;
        }
        if (wallet_key.empty()) continue;
        if (is_pre_balance) {
            tx_map[wallet_key][mint].pre = balance;
        } else {
            tx_map[wallet_key][mint].post = balance;
        }
    }
}

static bool check_array_for_hashes(
    std::string_view array_view,
    const std::unordered_set<uint64_t>& hashes_to_find,
    std::vector<std::string_view>& temp_buffer
) {
    parse_string_array(array_view, temp_buffer);
    for (const auto& addr : temp_buffer) {
        uint64_t hash = XXH3_64bits_withSeed(addr.data(), addr.length(), 0);
        if (hashes_to_find.count(hash)) {
            return true;
        }
    }
    return false;
}

// --- Transaction Finding (Unchanged) ---
std::vector<TxKeyViews> find_transaction_views(
    const std::vector<uint64_t>& index_list,
    const std::vector<size_t>& skipMap,
    const char* json_buffer
) {
    std::vector<TxKeyViews> transaction_views;
    const std::string_view K_READONLY = "\"readonly\":";
    const std::string_view K_WRITABLE = "\"writable\":";
    const std::string_view K_POST_TOKEN = "\"postTokenBalances\":";
    const std::string_view K_PRE_TOKEN = "\"preTokenBalances\":";
    const std::string_view K_ACCT_KEYS = "\"accountKeys\":";
    enum class ExpectState { Readonly, Writable, PostToken, PreToken, AccountKeys };
    ExpectState current_state = ExpectState::Readonly;
    TxKeyViews current_tx;
    auto get_view_from_index = [&](size_t i) -> std::string_view {
        size_t start_pos = index_list[i];
        size_t end_pos = index_list[skipMap[i]];
        return std::string_view(json_buffer + start_pos, end_pos - start_pos + 1);
    };
    for (size_t i = 0; i < index_list.size(); ++i) {
        if (json_buffer[index_list[i]] != '[') continue;

        size_t start_pos = index_list[i];

        if (start_pos > K_READONLY.length() &&
            memcmp(json_buffer + start_pos - K_READONLY.length(), K_READONLY.data(), K_READONLY.length()) == 0)
        {
            current_state = ExpectState::Readonly;
            current_tx = {};
            current_tx.readonly_view = get_view_from_index(i);
            current_state = ExpectState::Writable;
        }
        else if (current_state == ExpectState::Writable && start_pos > K_WRITABLE.length() &&
                 memcmp(json_buffer + start_pos - K_WRITABLE.length(), K_WRITABLE.data(), K_WRITABLE.length()) == 0)
        {
            current_tx.writable_view = get_view_from_index(i);
            current_state = ExpectState::PostToken;
        }
        else if (current_state == ExpectState::PostToken && start_pos > K_POST_TOKEN.length() &&
                 memcmp(json_buffer + start_pos - K_POST_TOKEN.length(), K_POST_TOKEN.data(), K_POST_TOKEN.length()) == 0)
        {
            current_tx.postTokenBalances_view = get_view_from_index(i);
            current_state = ExpectState::PreToken;
        }
        else if (current_state == ExpectState::PreToken && start_pos > K_PRE_TOKEN.length() &&
                 memcmp(json_buffer + start_pos - K_PRE_TOKEN.length(), K_PRE_TOKEN.data(), K_PRE_TOKEN.length()) == 0)
        {
            current_tx.preTokenBalances_view = get_view_from_index(i);
            current_state = ExpectState::AccountKeys;
        }
        else if (current_state == ExpectState::AccountKeys && start_pos > K_ACCT_KEYS.length() &&
                 memcmp(json_buffer + start_pos - K_ACCT_KEYS.length(), K_ACCT_KEYS.data(), K_ACCT_KEYS.length()) == 0)
        {
            current_tx.accountKeys_view = get_view_from_index(i);
            transaction_views.push_back(current_tx);
            current_state = ExpectState::Readonly;
        }
    }
    return transaction_views;
}

// --- HELPER: Define Output Schema for Flight ---
std::shared_ptr<arrow::Schema> GetOutputSchema() {
    return arrow::schema({
        arrow::field("wallet", arrow::utf8()),
        arrow::field("signature", arrow::utf8()),
        arrow::field("mint", arrow::utf8()),
        arrow::field("pre_balance", arrow::utf8()),
        arrow::field("post_balance", arrow::utf8())
    });
}

// --- HELPER: Flush Builders to Flight (Sends Block Time in Metadata) ---
arrow::Status FlushBuildersToFlight(
    ThreadLocalBuilders& builders,
    arrow::flight::FlightStreamWriter* writer,
    const std::string& block_time_str // <--- Renamed for clarity
) {
    std::shared_ptr<arrow::Array> wallet_arr;
    std::shared_ptr<arrow::Array> sig_arr;
    std::shared_ptr<arrow::Array> mint_arr;
    std::shared_ptr<arrow::Array> pre_arr;
    std::shared_ptr<arrow::Array> post_arr;

    ARROW_RETURN_NOT_OK(builders.tx_wallet_builder.Finish(&wallet_arr));
    ARROW_RETURN_NOT_OK(builders.signature_builder.Finish(&sig_arr));
    ARROW_RETURN_NOT_OK(builders.mint_builder.Finish(&mint_arr));
    ARROW_RETURN_NOT_OK(builders.pre_balance_builder.Finish(&pre_arr));
    ARROW_RETURN_NOT_OK(builders.post_balance_builder.Finish(&post_arr));

    // 1. Get Schema
    auto base_schema = GetOutputSchema();

    // 2. Create Batch
    auto batch = arrow::RecordBatch::Make(
        base_schema,
        wallet_arr->length(),
        {wallet_arr, sig_arr, mint_arr, pre_arr, post_arr}
    );

    if (batch->num_rows() > 0) {
        // 3. Create Metadata Buffer with Block Time
        // Key is still "timestamp" so Python picks it up automatically
        std::string meta_payload = "timestamp:" + block_time_str;
        std::shared_ptr<arrow::Buffer> meta_buffer = arrow::Buffer::FromString(meta_payload);
        
        // 4. Send Batch WITH Metadata
        ARROW_RETURN_NOT_OK(writer->WriteWithMetadata(*batch, meta_buffer));
    }
    return arrow::Status::OK();
}

// --- MODIFIED: Process Transactions Parallel with Flight ---
void process_transactions_parallel(
    const std::vector<TxKeyViews>& transaction_views,
    const HotAddressLookups& hot_addresses,
    const std::string& block_time_str, // <--- Renamed: Pass your found blockTime here
    const std::string& flight_server_uri,
    std::atomic<size_t>& pool_tx_counter
) {
    const unsigned int num_threads = std::max(1u, std::thread::hardware_concurrency());
    std::cout << "Using " << num_threads << " threads for Pass 2 (Direct to Flight)." << std::endl;

    std::vector<std::thread> threads;
    size_t num_transactions = transaction_views.size();
    size_t chunk_size = (num_transactions + num_threads - 1) / num_threads;
    const std::string data_center_index = "-1";

    const int64_t BATCH_SIZE_THRESHOLD = 10000;

    auto worker_fn = [&](size_t thread_id) {
        ThreadLocalBuilders builders;
        OwnerMintMap tx_map;
        std::vector<std::string_view> address_strings;
        std::unordered_map<int, std::string_view> index_to_hot_addr_map;

        auto location_result = arrow::flight::Location::Parse(flight_server_uri);
        if (!location_result.ok()) {
            std::cerr << "Thread " << thread_id << " Location Error: " << location_result.status().ToString() << std::endl;
            return;
        }
        arrow::flight::Location location = *location_result;

        auto client_result = arrow::flight::FlightClient::Connect(location, arrow::flight::FlightClientOptions());
        if (!client_result.ok()) {
            std::cerr << "Thread " << thread_id << " Connect Error: " << client_result.status().ToString() << std::endl;
            return;
        }
        std::unique_ptr<arrow::flight::FlightClient> client = std::move(*client_result);

        auto descriptor = arrow::flight::FlightDescriptor::Path({"solana_data"});
        auto schema = GetOutputSchema();

        auto doput_result = client->DoPut(arrow::flight::FlightCallOptions(), descriptor, schema);
        if (!doput_result.ok()) {
            std::cerr << "Thread " << thread_id << " DoPut Error: " << doput_result.status().ToString() << std::endl;
            return;
        }

        std::unique_ptr<arrow::flight::FlightStreamWriter> writer = std::move(doput_result->writer);
        std::unique_ptr<arrow::flight::FlightMetadataReader> reader = std::move(doput_result->reader);

        size_t start_tx = thread_id * chunk_size;
        size_t end_tx = std::min(start_tx + chunk_size, num_transactions);

        for (size_t tx_idx = start_tx; tx_idx < end_tx; ++tx_idx) {
            const auto& tx_views = transaction_views[tx_idx];

            bool is_pool_tx =
               check_array_for_hashes(tx_views.accountKeys_view, hot_addresses.pool_hashes, address_strings) ||
               check_array_for_hashes(tx_views.writable_view, hot_addresses.pool_hashes, address_strings) ||
               check_array_for_hashes(tx_views.readonly_view, hot_addresses.pool_hashes, address_strings);

            if (!is_pool_tx) continue;
            pool_tx_counter++;

            index_to_hot_addr_map.clear();
            int current_index = 0;

            parse_string_array(tx_views.accountKeys_view, address_strings);
            for (const auto& addr : address_strings) {
                const uint64_t hash = XXH3_64bits_withSeed(addr.data(), addr.length(), 0);
                if (hot_addresses.pool_hashes.count(hash)) index_to_hot_addr_map[current_index] = hot_addresses.hash_to_addr_map.at(hash);
                current_index++;
            }
            parse_string_array(tx_views.writable_view, address_strings);
            for (const auto& addr : address_strings) {
                const uint64_t hash = XXH3_64bits_withSeed(addr.data(), addr.length(), 0);
                if (hot_addresses.pool_hashes.count(hash)) index_to_hot_addr_map[current_index] = hot_addresses.hash_to_addr_map.at(hash);
                current_index++;
            }
            parse_string_array(tx_views.readonly_view, address_strings);
            for (const auto& addr : address_strings) {
                const uint64_t hash = XXH3_64bits_withSeed(addr.data(), addr.length(), 0);
                if (hot_addresses.pool_hashes.count(hash)) index_to_hot_addr_map[current_index] = hot_addresses.hash_to_addr_map.at(hash);
                current_index++;
            }

            tx_map.clear();
            parse_token_balance_array(tx_views.preTokenBalances_view, tx_map, true, index_to_hot_addr_map);
            parse_token_balance_array(tx_views.postTokenBalances_view, tx_map, false, index_to_hot_addr_map);

            // Use the block_time_str in the signature generation
            std::string signature = block_time_str + "-" + std::to_string(tx_idx) + data_center_index;
            for (const auto& owner_pair : tx_map) {
                const std::string_view& owner_wallet = owner_pair.first;
                for (const auto& mint_pair : owner_pair.second) {
                    const std::string_view& mint = mint_pair.first;
                    const BalancePair& balances = mint_pair.second;

                    if (!balances.pre.empty() || !balances.post.empty()) {
                        builders.AppendRow(owner_wallet, signature, mint, balances.pre, balances.post);
                    }
                }
            }

            if (builders.tx_wallet_builder.length() >= BATCH_SIZE_THRESHOLD) {
                // Pass block_time_str to Flush
                arrow::Status status = FlushBuildersToFlight(builders, writer.get(), block_time_str);
                if (!status.ok()) {
                     std::cerr << "Thread " << thread_id << " Write Error: " << status.ToString() << std::endl;
                     break;
                }
            }
        }

        if (builders.tx_wallet_builder.length() > 0) {
             // Pass block_time_str to Final Flush
             arrow::Status status = FlushBuildersToFlight(builders, writer.get(), block_time_str);
             if (!status.ok()) {
                 std::cerr << "Thread " << thread_id << " Final Flush Error: " << status.ToString() << std::endl;
             }
        }

        arrow::Status status = writer->DoneWriting();
        if (!status.ok()) std::cerr << "DoneWriting Error: " << status.ToString() << std::endl;

        status = writer->Close();
        if (!status.ok()) std::cerr << "Close Error: " << status.ToString() << std::endl;
    };

    for (unsigned int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker_fn, i);
    }
    for (auto& t : threads) {
        t.join();
    }
}
