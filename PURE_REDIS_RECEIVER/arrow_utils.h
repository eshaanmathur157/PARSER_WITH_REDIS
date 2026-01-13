#pragma once

#include "common.h"

// Consolidates results from all threads into one Arrow Table
std::shared_ptr<arrow::Table> consolidate_arrow_results(
    std::vector<ThreadLocalBuilders>& thread_builders,
    const std::string& block_time_str
);

// Prints the final table and metadata
void print_final_table(const std::shared_ptr<arrow::Table>& table);