/*******************************************************************************
 * thrill/core/reduce_checker.hpp
 *
 * Probabilistic reduce checker
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_CHECKER_HEADER
#define THRILL_CORE_REDUCE_CHECKER_HEADER

#include <array>
#include <utility>

#include <thrill/api/context.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_functional.hpp>

namespace thrill {
namespace core {

namespace _detail {
//! Reduce checker minireduction helper
template <typename Key, typename Value, typename ReduceFunction,
          typename hash_fn = common::hash_crc32<Key>,
          size_t bucket_bits = 2>
class ReduceCheckerMinireduction {
    static_assert(reduce_checkable_v<ReduceFunction>,
                  "Reduce function isn't checkable");

    using KeyValuePair = std::pair<Key, Value>;
    //! hash value type
    using hash_t = decltype(hash_fn()(Key{}));
    //! Bits in hash value
    static constexpr size_t hash_bits = 8 * sizeof(hash_t);
    static_assert(bucket_bits <= hash_bits,
                  "hash_fn produces fewer bits than needed to discern buckets");

    //! Number of parallel executions
    static constexpr size_t num_parallel = hash_bits / bucket_bits;
    //! Number of buckets
    static constexpr size_t num_buckets = 1ULL << bucket_bits;
    //! Mask to extract a bucket
    static constexpr size_t bucket_mask = (1ULL << bucket_bits) - 1;

    using reduction_t = std::array<Value, num_buckets>;

public:
    ReduceCheckerMinireduction() {
        reset();
    }

    //! Reset minireduction to initial state
    void reset() {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::fill(_reductions[i].begin(), _reductions[i].end(), Value{});
        }
    }

    //! Add a single item with Key key and Value value
    void push(const Key &key, const Value &value) {
        hash_t h = hash(key);
        for (size_t idx = 0; idx < num_parallel; ++idx) {
            size_t bucket = extract_bucket(h, idx);
            update_bucket(idx, bucket, value);
        }
    }

    //! Compare for equality
    template <typename Other>
    bool operator==(const Other &other) const {
        LOG1 << "minired() operator==";
        // check dimensions
        if (num_buckets != other.num_buckets) return false;
        if (num_parallel != other.num_parallel) return false;
        // check all buckets for equality
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                if (_reductions[i][j] != other._reductions[i][j])
                    return false;
            }
        }
        return true;
    }

    void all_reduce(api::Context &ctx) {
        _reductions = ctx.net.AllReduce(_reductions,
            common::ComponentSum<decltype(_reductions), ReduceFunction>(reduce));
    }

private:
    constexpr size_t extract_bucket(const hash_t &hash, size_t idx) {
        assert(idx < num_parallel);
        return (hash >> (idx * bucket_bits)) & bucket_mask;
    }

    void update_bucket(const size_t idx, const size_t bucket,
                       const Value &value) {
        _reductions[idx][bucket] = reduce(_reductions[idx][bucket], value);
    }

    std::array<reduction_t, num_parallel> _reductions;
    hash_fn hash;
    ReduceFunction reduce;
};
} // namespace _detail

//! Whether to check reductions (when applicable)
static constexpr bool check_reductions = true;

//! Reduce checker - no-op for unsupported reduce functions
template<typename Key, typename Value, typename ReduceFunction,
         typename Enable = void>
class ReduceChecker {
    using KeyValuePair = std::pair<Key, Value>;
public:
    void add_pre(const Key&, const Value&) {}
    void add_pre(const KeyValuePair&) {}
    void add_post(const Key&, const Value&) {}
    void add_post(const KeyValuePair&) {}
    bool check(api::Context&) { return true; }
};

/*!
 * Reduce checker for supported reduce functions
 */
template<typename Key, typename Value, typename ReduceFunction>
class ReduceChecker<Key, Value, ReduceFunction,
                    typename std::enable_if_t<check_reductions &&
                                              reduce_checkable_v<ReduceFunction>>>
{
    using KeyValuePair = std::pair<Key, Value>;
public:
    void add_pre(const Key &key, const Value &value) {
        mini_pre.push(key, value);
    }
    void add_pre(const KeyValuePair &kv) {
        mini_pre.push(kv.first, kv.second);
    }

    void add_post(const Key &key, const Value &value) {
        mini_post.push(key, value);
    }
    void add_post(const KeyValuePair &kv) {
        mini_post.push(kv.first, kv.second);
    }

    bool check(api::Context & ctx) {
        LOG1 << "Checking reduction...";
        mini_pre.all_reduce(ctx);
        mini_post.all_reduce(ctx);
        bool success = (mini_pre == mini_post);
        LOG1 << "check(): " << (success ? "yay" : "NAY");
        return success;
    }
private:
    _detail::ReduceCheckerMinireduction<Key, Value, ReduceFunction>
        mini_pre, mini_post;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_CHECKER_HEADER

/******************************************************************************/
