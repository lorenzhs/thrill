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

#include <thrill/api/context.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_functional.hpp>

#include <array>
#include <utility>

namespace thrill {
namespace core {

namespace _detail {
//! Reduce checker minireduction helper
template <typename Key, typename Value, typename ReduceFunction,
          typename hash_fn = common::hash_crc32<Key>,
          size_t bucket_bits = 3>
class ReduceCheckerMinireduction
{
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

    //! Enable extra debug output by setting this to true
    static constexpr bool extra_verbose = false;

public:
    ReduceCheckerMinireduction() {
        reset();
    }

    //! Reset minireduction to initial state
    void reset() {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::fill(reductions_[i].begin(), reductions_[i].end(), Value{});
        }
    }

    //! Add a single item with Key key and Value value
    void push(const Key& key, const Value& value) {
        hash_t h = hash_(key);
        for (size_t idx = 0; idx < num_parallel; ++idx) {
            size_t bucket = extract_bucket(h, idx);
            update_bucket(idx, bucket, value);
        }
    }

    //! Compare for equality
    template <typename Other>
    bool operator == (const Other& other) const {
        // check dimensions
        if (num_buckets != other.num_buckets) return false;
        if (num_parallel != other.num_parallel) return false;
        // check all buckets for equality
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                if (reductions_[i][j] != other.reductions_[i][j])
                    return false;
            }
        }
        return true;
    }

    void all_reduce(api::Context& ctx) {
        reductions_ = ctx.net.AllReduce(reductions_,
            common::ComponentSum<decltype(reductions_), ReduceFunction>(reduce));

        if (extra_verbose && ctx.net.my_rank() == 0) {
            for (size_t i = 0; i < num_parallel; ++i) {
                std::stringstream s;
                s << "Run " << i << ": ";
                for (size_t j = 0; j < num_buckets; ++j) {
                    s << reductions_[i][j] << " ";
                }
                LOG1 << s.str();
            }
        }
    }

private:
    constexpr size_t extract_bucket(const hash_t& hash, size_t idx) {
        assert(idx < num_parallel);
        return (hash >> (idx * bucket_bits)) & bucket_mask;
    }

    void update_bucket(const size_t idx, const size_t bucket,
                       const Value& value) {
        reductions_[idx][bucket] = reduce(reductions_[idx][bucket], value);
    }

    std::array<reduction_t, num_parallel> reductions_;
    hash_fn hash_;
    ReduceFunction reduce;
};

} // namespace _detail

namespace checkers {

//! Whether to check reductions (when applicable)
static constexpr bool checkreductions_ = true;

//! Reduce checker - no-op for unsupported reduce functions
template <typename Key, typename Value, typename ReduceFunction,
          typename Enable = void>
class ReduceChecker
{
    using KeyValuePair = std::pair<Key, Value>;

public:
    void add_pre(const Key&, const Value&) { }
    void add_pre(const KeyValuePair&) { }
    void add_post(const Key&, const Value&) { }
    void add_post(const KeyValuePair&) { }
    bool check(api::Context&) { return true; }
};

/*!
 * Reduce checker for supported reduce functions
 */
template <typename Key, typename Value, typename ReduceFunction>
class ReduceChecker<Key, Value, ReduceFunction,
                    typename std::enable_if_t<checkreductions_&&
                                              reduce_checkable_v<ReduceFunction> > >
{
    using KeyValuePair = std::pair<Key, Value>;

public:
    void add_pre(const Key& key, const Value& value) {
        mini_pre.push(key, value);
    }
    void add_pre(const KeyValuePair& kv) {
        mini_pre.push(kv.first, kv.second);
    }

    void add_post(const Key& key, const Value& value) {
        mini_post.push(key, value);
    }
    void add_post(const KeyValuePair& kv) {
        mini_post.push(kv.first, kv.second);
    }

    bool check(api::Context& ctx) {
        mini_pre.all_reduce(ctx);
        mini_post.all_reduce(ctx);
        bool success = (mini_pre == mini_post);
        LOGC(ctx.my_rank() == 0) << "check(): " << (success ? "yay" : "NAY");
        return success;
    }

private:
    _detail::ReduceCheckerMinireduction<Key, Value, ReduceFunction>
        mini_pre, mini_post;
};

//! Debug manipulators?
static constexpr bool debug = false;

//! Dummy No-Op Reduce Manipulator
struct ReduceManipulatorDummy {
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        return std::make_pair(begin, end);
    }
    bool made_changes() const { return false; }
};

//! Drops first element
struct ReduceManipulatorDropFirst {
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        if (begin < end) {
            sLOG << "Manipulating" << end - begin << "elements, dropping first";
            // << *begin;
            made_changes_ = true;
            return std::make_pair(begin + 1, end);
        } else {
            return std::make_pair(begin, end);
        }
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Increments value of first element
struct ReduceManipulatorIncFirst {
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        if (begin < end) {
            sLOG << "Manipulating" << end - begin
                 << "elements, incrementing first";
            // << *begin;
            begin->second++;
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Increments key of first element
struct ReduceManipulatorIncFirstKey {
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        if (begin < end) {
            sLOG << "Manipulating" << end - begin << "elements, incrementing key";
            // << *begin;
            begin->first++;
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Switches values of first and second element
struct ReduceManipulatorSwitchValues {
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        if (begin + 1 < end && begin->second != (begin + 1)->second) {
            sLOG << "Manipulating" << end - begin << "elements, switching values";
            // << *begin << *(begin+1);
            auto tmp = begin->second;
            begin->second = (begin + 1)->second;
            (begin + 1)->second = tmp;
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

} // namespace checkers
} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_CHECKER_HEADER

/******************************************************************************/
