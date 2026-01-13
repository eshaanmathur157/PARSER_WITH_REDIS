#include "file_utils.h"

void load_hot_addresses(const char* filepath, HotAddressLookups& lookups) {
    try {
        std::ifstream bq_file(filepath);
        if (!bq_file.is_open()) {
            std::cerr << "Error opening base/quotes file: " << filepath << std::endl;
            exit(1);
        }
        
        std::string line;
        while (std::getline(bq_file, line)) {
            if (line.empty()) continue;
            
            // Use the passed 'lookups' variable directly
            lookups.storage.push_back(line);
            std::string_view addr_view = lookups.storage.back();
            uint64_t hash = XXH3_64bits_withSeed(addr_view.data(), addr_view.length(), 0);
            
            lookups.pool_hashes.insert(hash);
            lookups.hash_to_addr_map[hash] = addr_view;
        }
        
        std::cout << "Loaded " << lookups.storage.size() << " hot addresses." << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "File I/O Error: " << e.what() << std::endl;
        exit(1);
    }
    // No return statement needed
}

std::vector<char> load_json_file(const char* filepath) {
    std::vector<char> json_buffer;
    std::streamsize file_size = 0;

    try {
        std::ifstream file(filepath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            std::cerr << "Error opening file: " << filepath << std::endl;
            exit(1);
        }
        file_size = file.tellg();
        file.seekg(0, std::ios::beg);
        if (file_size <= 0) {
             std::cerr << "Error getting file size or file is empty." << std::endl;
             exit(1);
        }
        json_buffer.resize(static_cast<size_t>(file_size) + 32); // Add AVX padding
        if (!file.read(json_buffer.data(), file_size)) {
            std::cerr << "Error reading file." << std::endl;
            exit(1);
        }
        memset(json_buffer.data() + static_cast<size_t>(file_size), 0, 32);
    
    } catch (const std::exception& e) {
        std::cerr << "File I/O Error: " << e.what() << std::endl;
        exit(1);
    }
    return json_buffer;
}

std::string extract_block_time(const std::string_view& json_view) {
    std::string block_time_str;
    try {
        const std::string_view block_time_key = "\"blockTime\":";
        size_t key_pos = json_view.find(block_time_key);
        if (key_pos != std::string_view::npos) {
            size_t value_start = key_pos + block_time_key.length();
            size_t value_end = json_view.find(',', value_start);
            if (value_end != std::string_view::npos) {
                block_time_str.assign(json_view.substr(value_start, value_end - value_start));
                std::cout << "Found blockTime: " << block_time_str << std::endl;
            }
        }
        if (block_time_str.empty()) {
            std::cerr << "Warning: Could not find 'blockTime' in JSON." << std::endl;
        }
    } catch (const std::exception& e) {
        std::cerr << "Warning: Error parsing blockTime: " << e.what() << std::endl;
    }
    return block_time_str;
}
