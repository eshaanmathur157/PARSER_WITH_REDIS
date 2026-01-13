// parser_main.cpp

// C++ Standard Library
#include <iostream>
#include <string_view>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring> // For memcpy, strerror
#include <thread>
#include <chrono>
#include <atomic>
#include <exception>
#include <mutex> // <--- Added for std::mutex

// Linux/POSIX Shared Memory Headers
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Your Parser Project Headers
#include "common.h"
#include "file_utils.h"
#include "stage1_simd.h"
#include "stage2_processing.h"
#include "arrow_utils.h"
#include <sw/redis++/redis++.h> // wrapper for Redis
#include <nlohmann/json.hpp>    // strictly for parsing the Redis JSON msg easily

// --- Shared Memory Configuration ---
const char* SHM_NAME = "solana_json_shm";
const size_t SHM_SIZE = 10 * 1024 * 1024; // 10MB

using json = nlohmann::json;
using namespace sw::redis;

// Offsets matches your Python script
const size_t FLAG_OFFSET = 0;
const size_t SIZE_OFFSET = 1;
const size_t DATA_OFFSET = 9;

// --- FLIGHT SERVER CONFIGURATION ---
const std::string FLIGHT_SERVER_URI = "grpc://20.46.50.39:8815";

// --- GLOBAL MUTEX ---
std::mutex global_hot_addr_mutex; // <--- The Global Lock

void redis_listener_thread(HotAddressLookups& hot_addrs) {
    std::cout << "[Redis Listener] Thread started. TID = "
              << std::this_thread::get_id() << std::endl;

    try {
        std::cout << "[Redis Listener] Connecting to Redis..." << std::endl;

        auto redis = Redis("tcp://20.46.50.39:6379");

        std::cout << "[Redis Listener] Connected to Redis." << std::endl;

        auto sub = redis.subscriber();

        std::atomic<uint64_t> msg_count{0};

        sub.on_message([&hot_addrs, &msg_count](std::string channel, std::string msg) {
            msg_count++;
            std::cout << "[Redis Listener] Message #" << msg_count.load()
                      << " received on channel: " << channel << std::endl;

            try {
                auto j = json::parse(msg);

                std::string base_vault  = j["base_vault"];
                std::string quote_vault = j["quote_vault"];

                // --- CRITICAL SECTION START ---
                std::cout << "[Redis Listener] Acquiring lock to update vaults..." << std::endl;
                {
                    std::lock_guard<std::mutex> lock(global_hot_addr_mutex);
                    hot_addrs.add_new_vaults(base_vault, quote_vault);
                } // Mutex releases automatically here
                std::cout << "[Redis Listener] Updated hot vaults: "
                          << base_vault << " , " << quote_vault 
                          << ". Lock released." << std::endl;
                // --- CRITICAL SECTION END ---

            } catch (const std::exception& e) {
                std::cerr << "[Redis Listener ERROR] JSON parse failed: "
                          << e.what() << std::endl;
            }
        });

        std::cout << "[Redis Listener] Subscribing to channel: pool-monitor" << std::endl;
        sub.subscribe("pool-monitor");
        std::cout << "[Redis Listener] Successfully subscribed." << std::endl;

        auto last_heartbeat = std::chrono::steady_clock::now();

        while (true) {
            // 🔴 THIS CALL BLOCKS — thread will sleep in epoll/futex
            sub.consume();

            // Heartbeat every 5 seconds (only prints if consume wakes up)
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_heartbeat).count() >= 5) {
                std::cout << "[Redis Listener] Alive. Messages so far: "
                          << msg_count.load() << std::endl;
                last_heartbeat = now;
            }
        }

    } catch (const Error& e) {
        std::cerr << "[Redis Listener CRITICAL] Redis error: "
                  << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[Redis Listener CRITICAL] Std exception: "
                  << e.what() << std::endl;
    } catch (...) {
        std::cerr << "[Redis Listener CRITICAL] Unknown exception." << std::endl;
    }

    std::cerr << "[Redis Listener] Thread exiting unexpectedly!" << std::endl;
}

// ----------------------------------
int main(int argc, char *argv[]) {
    // --- 1. Setup Hot Addresses ---
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <base_quotes_file>" << std::endl;
        return 1;
    }

    std::cout << "Loading hot addresses from: " << argv[1] << std::endl;
    HotAddressLookups hot_addresses; 
    load_hot_addresses(argv[1], hot_addresses);
    std::cout << "Loaded hot addresses." << std::endl;
    
    std::thread redis_thread(redis_listener_thread, std::ref(hot_addresses));
    redis_thread.detach();
    
    // --- 2. Connect to Shared Memory ---
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd == -1) {
        std::cerr << "Failed to open SHM: " << SHM_NAME << "\nError: " << strerror(errno) << std::endl;
        std::cerr << "Make sure the Python script is running first!" << std::endl;
        return 1;
    }
    char* pBuf = (char*)mmap(0, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (pBuf == MAP_FAILED) {
        std::cerr << "mmap failed: " << strerror(errno) << std::endl;
        close(shm_fd);
        return 1;
    }
    close(shm_fd); // FD not needed after mapping
    std::cout << "Consumer: Attached to shared memory. Polling..." << std::endl;

    // --- 3. Pointers to Offsets ---
    volatile uint8_t* flag_ptr = reinterpret_cast<volatile uint8_t*>(pBuf + FLAG_OFFSET);
    char* data_start_ptr = pBuf + DATA_OFFSET;

    // --- 4. Main Loop ---
    try {
        while (true) {
            // Python writes 1 when data is ready. We wait for 1.
            if (*flag_ptr == 1) {

                auto job_start_time = std::chrono::high_resolution_clock::now();
                
                // --- READ SIZE SAFELY ---
                uint64_t json_size = 0;
                std::memcpy(&json_size, pBuf + SIZE_OFFSET, 8); 

                // Safety Check
                if (json_size > (SHM_SIZE - DATA_OFFSET)) {
                    std::cerr << "Error: Size in SHM (" << json_size << ") exceeds buffer limit." << std::endl;
                    *flag_ptr = 0; // Reset and skip
                    continue;
                }
                std::cout << "\n>>> New Data Detected! Size: " << json_size << " bytes" << std::endl;

                // --- PROCESS ONLY THE JSON PART ---
                
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

                // FIXED: Passing 'block_time_str' extracted from JSON instead of current system time
                // PASSING THE GLOBAL MUTEX BY REFERENCE HERE
                process_transactions_parallel(
                    transaction_views,
                    hot_addresses,
                    block_time_str,      
                    FLIGHT_SERVER_URI,
                    pool_tx_counter,
                    global_hot_addr_mutex // <--- Passing the mutex
                );

                // Optional: Print summary
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
        *flag_ptr = 0; // Reset flag on crash
    }
    munmap(pBuf, SHM_SIZE);
    return 0;
}