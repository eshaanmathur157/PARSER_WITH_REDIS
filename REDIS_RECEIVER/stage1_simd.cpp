#include "stage1_simd.h"
#include <immintrin.h> // SIMD intrinsics

// This helper is only used by process_json_in_batches, so it stays here.
inline size_t store_mask_indices(uint64_t* list_ptr, uint32_t mask, uint64_t offset) {
    size_t count = 0;
    while (mask != 0) {
        unsigned int index = _tzcnt_u32(mask);
        list_ptr[count] = (uint64_t)offset + index;
        count++;
        mask = _blsr_u32(mask); // Reset lowest set bit
    }
    return count;
}

// The full, unchanged SIMD function
size_t process_json_in_batches(const char* json_string, size_t len, uint64_t* index_list_ptr) {
    uint32_t prev_R_carry_mask = 0;
    uint32_t prev_SW_last_bit = 0;
    size_t offset = 0;
    size_t current_index_count = 0;
    const uint32_t full_len_mask = 0xFFFFFFFF;
    const __m128i T_low_128 = _mm_setr_epi8(16, 0, 0, 0, 0, 0, 0, 0, 0, 8, 10, 4, 1, 12, 0, 0);
    const __m128i T_high_128 = _mm_setr_epi8(8, 0, 17, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    const __m256i T_low = _mm256_broadcastsi128_si256(T_low_128);
    const __m256i T_high = _mm256_broadcastsi128_si256(T_high_128);
    const __m256i low_nibble_mask = _mm256_set1_epi8(0x0F);
    const __m256i quote_mask_reg = _mm256_set1_epi8('"');
    const __m256i S_mask_reg = _mm256_set1_epi8(4);
    const __m256i W_reg_result = _mm256_set1_epi8(24);
    const __m256i zero_reg = _mm256_setzero_si256();
    const __m256i all_ones = _mm256_set1_epi8(-1);
    const __m128i v_ones = _mm_set1_epi8(-1);
    size_t fast_len = len & ~size_t(31);
    
    // ... (The entire rest of the function, unchanged) ...
    // ---
    while (offset < fast_len) {
        __m256i input_chars = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(json_string + offset));
        __m256i quote_cmp_result = _mm256_cmpeq_epi8(input_chars, quote_mask_reg);
        uint32_t Q = (uint32_t)_mm256_movemask_epi8(quote_cmp_result);
        __m256i low_nibbles = _mm256_and_si256(input_chars, low_nibble_mask);
        __m256i high_nibbles_shifted = _mm256_srli_epi16(input_chars, 4);
        __m256i high_nibbles = _mm256_and_si256(high_nibbles_shifted, low_nibble_mask);
        __m256i low_results = _mm256_shuffle_epi8(T_low, low_nibbles);
        __m256i high_results = _mm256_shuffle_epi8(T_high, high_nibbles);
        __m256i class_codes = _mm256_and_si256(low_results, high_results);
        __m256i S_reg_result = _mm256_and_si256(class_codes, S_mask_reg);
        __m256i W_reg_result_masked = _mm256_and_si256(class_codes, W_reg_result); // Renamed
        __m256i S_zero_cmp = _mm256_cmpeq_epi8(S_reg_result, zero_reg);
        __m256i W_zero_cmp = _mm256_cmpeq_epi8(W_reg_result_masked, zero_reg); // Renamed
        __m256i S_nonzero_reg = _mm256_xor_si256(S_zero_cmp, all_ones);
        __m256i W_nonzero_reg = _mm256_xor_si256(W_zero_cmp, all_ones);
        uint32_t S = (uint32_t)_mm256_movemask_epi8(S_nonzero_reg);
        uint32_t W = (uint32_t)_mm256_movemask_epi8(W_nonzero_reg);
        uint32_t Q_un = Q;
        __m128i v_q = _mm_set_epi32(0, 0, 0, Q_un);
        __m128i v_res = _mm_clmulepi64_si128(v_q, v_ones, 0x00);
        uint32_t R_local = (uint32_t)_mm_cvtsi128_si32(v_res);
        uint32_t R = R_local ^ prev_R_carry_mask;
        S = S & ~R;
        uint32_t SW = S | W;
        uint32_t P = (SW << 1) | prev_SW_last_bit;
        P = P & ~W & ~R;
        P = P & full_len_mask;
        S = S & full_len_mask;
        current_index_count += store_mask_indices(index_list_ptr + current_index_count, S, offset);
        uint32_t Q_count_is_odd = (R_local >> 31) & 1;
        if (Q_count_is_odd) {
            prev_R_carry_mask = ~prev_R_carry_mask;
        }
        prev_SW_last_bit = (SW >> 31) & 1;
        offset += 32;
    }
    while (offset < len) {
        alignas(32) char buffer[32];
        size_t bytes_to_copy = (len - offset);
        memcpy(buffer, json_string + offset, bytes_to_copy);
        memset(buffer + bytes_to_copy, 0, 32 - bytes_to_copy);
        uint32_t len_mask = (bytes_to_copy > 0) ? (1U << bytes_to_copy) - 1 : 0;
        __m256i input_chars = _mm256_load_si256(reinterpret_cast<const __m256i*>(buffer));
        __m256i quote_cmp_result = _mm256_cmpeq_epi8(input_chars, quote_mask_reg);
        uint32_t Q = (uint32_t)_mm256_movemask_epi8(quote_cmp_result);
        __m256i low_nibbles = _mm256_and_si256(input_chars, low_nibble_mask);
        __m256i high_nibbles_shifted = _mm256_srli_epi16(input_chars, 4);
        __m256i high_nibbles = _mm256_and_si256(high_nibbles_shifted, low_nibble_mask);
        __m256i low_results = _mm256_shuffle_epi8(T_low, low_nibbles);
        __m256i high_results = _mm256_shuffle_epi8(T_high, high_nibbles);
        __m256i class_codes = _mm256_and_si256(low_results, high_results);
        __m256i S_reg_result = _mm256_and_si256(class_codes, S_mask_reg);
        __m256i W_reg_result_masked = _mm256_and_si256(class_codes, W_reg_result); // Renamed
        __m256i S_zero_cmp = _mm256_cmpeq_epi8(S_reg_result, zero_reg);
        __m256i W_zero_cmp = _mm256_cmpeq_epi8(W_reg_result_masked, zero_reg); // Renamed
        __m256i S_nonzero_reg = _mm256_xor_si256(S_zero_cmp, all_ones);
        __m256i W_nonzero_reg = _mm256_xor_si256(W_zero_cmp, all_ones);
        uint32_t S = (uint32_t)_mm256_movemask_epi8(S_nonzero_reg);
        uint32_t W = (uint32_t)_mm256_movemask_epi8(W_nonzero_reg);
        uint32_t Q_un = Q;
        __m128i v_q = _mm_set_epi32(0, 0, 0, Q_un);
        __m128i v_res = _mm_clmulepi64_si128(v_q, v_ones, 0x00);
        uint32_t R_local = (uint32_t)_mm_cvtsi128_si32(v_res);
        uint32_t R = R_local ^ prev_R_carry_mask;
        S = S & ~R;
        uint32_t SW = S | W;
        uint32_t P = (SW << 1) | prev_SW_last_bit;
        P = P & ~W & ~R;
        P = P & len_mask;
        S = S & len_mask;
        current_index_count += store_mask_indices(index_list_ptr + current_index_count, S, offset);
        
        uint32_t Q_masked = Q_un & len_mask;
        __m128i v_q_masked = _mm_set_epi32(0, 0, 0, Q_masked);
        __m128i v_res_masked = _mm_clmulepi64_si128(v_q_masked, v_ones, 0x00);
        uint32_t R_local_masked = (uint32_t)_mm_cvtsi128_si32(v_res_masked);
        
        uint32_t Q_count_is_odd = (R_local_masked >> 31) & 1;
        if (Q_count_is_odd) {
            prev_R_carry_mask = ~prev_R_carry_mask;
        }
        if (bytes_to_copy > 0) {
            prev_SW_last_bit = (SW >> (bytes_to_copy - 1)) & 1;
        } else {
            prev_SW_last_bit = 0;
        }
        offset += bytes_to_copy;
    }
    // ---
    
    return current_index_count;
}