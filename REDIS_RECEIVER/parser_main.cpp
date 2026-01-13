// parser_main.cpp

// C++ Standard Library
#include <iostream>
#include <string_view>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring> 
#include <thread>
#include <chrono>
#include <atomic>
#include <exception>

// Linux/POSIX Shared Memory Headers
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// HTTP Client Header (Requires -lcurl)
#include <curl/curl.h>

// Your Parser Project Headers
#include "common.h"
#include "file_utils.h"
#include "stage1_simd.h"
#include "stage2_processing.h"
#include "arrow_utils.h"
#include <nlohmann/json.hpp> 

// --- SHARED MEMORY CONFIG ---
const char* SHM_NAME = "solana_json_shm";
const size_t SHM_SIZE = 10 * 1024 * 1024; // 10MB

// --- PROXY CONFIGURATION ---
const std::string PROXY_NAME = "proxy1"; // <--- CHANGE THIS per machine (proxy2, proxy3...)
const std::string POOL_SERVER_URL = "http://20.46.50.39:8080/new_pools";

// Offsets
const size_t FLAG_OFFSET = 0;
const size_t SIZE_OFFSET = 1;
const size_t DATA_OFFSET = 9;

// Flight Server
const std::string FLIGHT_SERVER_URI = "grpc://20.46.50.39:8815";

using json = nlohmann::json;

// --- CURL Helper: Write Callback ---
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

// --- POLLING FUNCTION ---
void check_new_pools(HotAddressLookups& hot_addrs) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;

    curl = curl_easy_init();
    if (curl) {
        struct curl_slist* headers = NULL;
        std::string header_val = "X-Machine-Name: " + PROXY_NAME;
        headers = curl_slist_append(headers, header_val.c_str());

        // Set URL and Headers
        curl_easy_setopt(curl, CURLOPT_URL, POOL_SERVER_URL.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        
        // Callback to capture response
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        
        // Fast Timeout (don't block the parser for too long)
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 500L); 

        res = curl_easy_perform(curl);
        
        if (res == CURLE_OK && !readBuffer.empty()) {
            try {
                auto j = json::parse(readBuffer);
                
                // Expecting a list of objects: [{"base_vault": "...", ...}, {...}]
                if (j.is_array() && !j.empty()) {
                    std::cout << "[HTTP] Received " << j.size() << " new pool updates." << std::endl;
                    
                    for (const auto& item : j) {
                        std::string base_v = item.value("base_vault", "");
                        std::string quote_v = item.value("quote_vault", "");

                        if (!base_v.empty() && !quote_v.empty()) {
                            hot_addrs.add_new_vaults(base_v, quote_v);
                            std::cout << "   -> Added: " << base_v << " / " << quote_v << std::endl;
                        }
                    }
                }
            } catch (const std::exception& e) {
                // Silent catch or simple log to avoid spamming console on bad JSON
                // std::cerr << "[HTTP Error] JSON Parse: " << e.what() << std::endl;
            }
        }
        
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
    }
}

// ----------------------------------
int main(int argc, char *argv[]) {
    // --- 0. Init CURL Global ---
    curl_global_init(CURL_GLOBAL_ALL);

    // --- 1. Setup Hot Addresses ---
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <base_quotes_file>" << std::endl;
        return 1;
    }

    std::cout << "Loading hot addresses from: " << argv[1] << std::endl;
    HotAddressLookups hot_addresses; 
    load_hot_addresses(argv[1], hot_addresses);
    std::cout << "Loaded hot addresses." << std::endl;

    // --- 2. Connect to Shared Memory ---
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd == -1) {
        std::cerr << "Failed to open SHM. Make sure Python is running.\n";
        return 1;
    }
    char* pBuf = (char*)mmap(0, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (pBuf == MAP_FAILED) {
        std::cerr << "mmap failed\n";
        close(shm_fd);
        return 1;
    }
    close(shm_fd); 
    std::cout << "Consumer: Attached to shared memory as " << PROXY_NAME << ". Polling..." << std::endl;

    // --- 3. Pointers ---
    volatile uint8_t* flag_ptr = reinterpret_cast<volatile uint8_t*>(pBuf + FLAG_OFFSET);
    char* data_start_ptr = pBuf + DATA_OFFSET;

    // Timer for HTTP Polling (Throttle)
    auto last_http_check = std::chrono::steady_clock::now();

    // --- 4. Main Loop ---
    try {
        while (true) {
            
            // --- A. POLL SERVER (Every 1 Second) ---
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_http_check).count() >= 1) {
                check_new_pools(hot_addresses);
                last_http_check = now;
            }

            // --- B. CHECK SHARED MEMORY ---
            if (*flag_ptr == 1) {

                auto job_start_time = std::chrono::high_resolution_clock::now();
                
                uint64_t json_size = 0;
                std::memcpy(&json_size, pBuf + SIZE_OFFSET, 8); 

                if (json_size > (SHM_SIZE - DATA_OFFSET)) {
                    std::cerr << "Error: Size in SHM exceeds limit." << std::endl;
                    *flag_ptr = 0; 
                    continue;
                }
                std::cout << "\n>>> New Data Detected! Size: " << json_size << " bytes" << std::endl;

                // 1. Extract Timestamp
                std::string block_time_str = extract_block_time(std::string_view(data_start_ptr, json_size));

                // 2. Stage 1: SIMD Indexing
                std::vector<uint64_t> index_list;
                index_list.resize((json_size / 14) + 256);
                size_t actual_indices = process_json_in_batches(data_start_ptr, json_size, index_list.data());
                index_list.resize(actual_indices);

                // 3. Stage 2: Build SkipMap
                SkipMapBuilder skip_map_builder(index_list, data_start_ptr);

                // 4. Stage 2: Find Transactions
                auto transaction_views = find_transaction_views(index_list, skip_map_builder.getSkipMap(), data_start_ptr);

                // 5. Stage 2: Parallel Parse AND Send to Flight
                std::atomic<size_t> pool_tx_counter(0);

                process_transactions_parallel(
                    transaction_views,
                    hot_addresses,
                    block_time_str, 
                    FLIGHT_SERVER_URI,
                    pool_tx_counter
                );

                std::cout << "Parsed and Sent " << pool_tx_counter.load() << " target transactions." << std::endl;

                auto job_end_time = std::chrono::high_resolution_clock::now();
                std::chrono::duration<double, std::milli> elapsed = job_end_time - job_start_time;
                std::cout << "Done in " << elapsed.count() << " ms." << std::endl;

                // --- SIGNAL COMPLETION ---
                *flag_ptr = 0;
            }
            
            // Sleep briefly to save CPU
            std::this_thread::sleep_for(std::chrono::microseconds(500));
        }
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        *flag_ptr = 0; 
    }
    
    munmap(pBuf, SHM_SIZE);
    curl_global_cleanup();
    return 0;
}