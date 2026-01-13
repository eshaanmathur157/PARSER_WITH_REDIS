#pragma once

#include "common.h"

// Loads the hot addresses from the file
HotAddressLookups load_hot_addresses(const char* filepath);

// Loads the main JSON file into a buffer
std::vector<char> load_json_file(const char* filepath);

// Extracts the blockTime from the JSON buffer
std::string extract_block_time(const std::string_view& json_view);