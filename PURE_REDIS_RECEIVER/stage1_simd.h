#pragma once

#include "common.h"

// The main SIMD JSON indexing function
size_t process_json_in_batches(const char* json_string, size_t len, uint64_t* index_list_ptr);