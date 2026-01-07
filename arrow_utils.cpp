#include "arrow_utils.h"

std::shared_ptr<arrow::Table> consolidate_arrow_results(
    std::vector<ThreadLocalBuilders>& thread_builders,
    const std::string& block_time_str
) {
    std::cout << "Consolidating Arrow results from threads..." << std::endl;
    
    // --- FIX: Corrected typo from shared_p to shared_ptr ---
    std::vector<std::shared_ptr<arrow::Array>> wallet_arrays;
    std::vector<std::shared_ptr<arrow::Array>> sig_arrays;
    std::vector<std::shared_ptr<arrow::Array>> mint_arrays;
    std::vector<std::shared_ptr<arrow::Array>> pre_arrays;
    std::vector<std::shared_ptr<arrow::Array>> post_arrays;

    for (auto& builders : thread_builders) {
        std::shared_ptr<arrow::Array> wallet_array;
        std::shared_ptr<arrow::Array> sig_array;
        std::shared_ptr<arrow::Array> mint_array;
        std::shared_ptr<arrow::Array> pre_array;
        std::shared_ptr<arrow::Array> post_array;

        ARROW_CHECK_OK(builders.tx_wallet_builder.Finish(&wallet_array));
        ARROW_CHECK_OK(builders.signature_builder.Finish(&sig_array));
        ARROW_CHECK_OK(builders.mint_builder.Finish(&mint_array));
        ARROW_CHECK_OK(builders.pre_balance_builder.Finish(&pre_array));
        ARROW_CHECK_OK(builders.post_balance_builder.Finish(&post_array));
        
        wallet_arrays.push_back(std::move(wallet_array));
        sig_arrays.push_back(std::move(sig_array));
        mint_arrays.push_back(std::move(mint_array));
        pre_arrays.push_back(std::move(pre_array));
        post_arrays.push_back(std::move(post_array));
    }

    auto final_wallets_res = arrow::Concatenate(wallet_arrays);
    auto final_sigs_res = arrow::Concatenate(sig_arrays);
    auto final_mints_res = arrow::Concatenate(mint_arrays);
    auto final_pres_res = arrow::Concatenate(pre_arrays);
    auto final_posts_res = arrow::Concatenate(post_arrays);

    auto final_wallets = final_wallets_res.ValueOrDie();
    auto final_sigs = final_sigs_res.ValueOrDie();
    auto final_mints = final_mints_res.ValueOrDie();
    auto final_pres = final_pres_res.ValueOrDie();
    auto final_posts = final_posts_res.ValueOrDie();

    // --- FIX: Corrected typo from shared_p to shared_ptr ---
    std::shared_ptr<arrow::KeyValueMetadata> metadata = nullptr;
    if (!block_time_str.empty()) {
        metadata = arrow::key_value_metadata({"blockTime"}, {block_time_str});
    }

    auto schema = arrow::schema({
        arrow::field("tx_wallet", arrow::utf8()),
        arrow::field("signature", arrow::utf8()),
        arrow::field("mint_addr", arrow::utf8()),
        arrow::field("preTokenBalance", arrow::utf8()),
        arrow::field("postTokenBalance", arrow::utf8())
    }, metadata);

    return arrow::Table::Make(schema, {
        final_wallets, 
        final_sigs, 
        final_mints, 
        final_pres, 
        final_posts
    });
}

void print_final_table(const std::shared_ptr<arrow::Table>& table) {
    std::cout << "Successfully consolidated " << table->num_rows() << " rows." << std::endl;
    std::cout << "\n--- Arrow Table ---" << std::endl;
    ARROW_CHECK_OK(arrow::PrettyPrint(*table, {}, &std::cout));

    std::cout << "\n--- Arrow Metadata ---" << std::endl;
    if (table->schema()->HasMetadata()) {
        std::cout << table->schema()->metadata()->ToString() << std::endl;
    } else {
        std::cout << "No metadata found." << std::endl;
    }
}