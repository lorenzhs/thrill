/*******************************************************************************
 * thrill/checkers/reduce.hpp
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
#ifndef THRILL_CHECKERS_REDUCE_HEADER
#define THRILL_CHECKERS_REDUCE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/checkers/functional.hpp>
#include <thrill/checkers/manipulator.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

#include <array>
#include <random>
#include <utility>

namespace thrill {
namespace checkers {

namespace _detail {
//! Reduce checker minireduction helper
template <typename Key, typename Value, typename ReduceFunction,
          typename hash_fn = common::hash_crc32<Key>, size_t bucket_bits = 3>
class ReduceCheckerMinireduction : public noncopynonmove
{
    static_assert(reduce_checkable_v<ReduceFunction>,
                  "Reduce function isn't (marked) checkable");

    using KeyValuePair = std::pair<Key, Value>;
    //! hash value type
    using hash_t = decltype(hash_fn()(Key { }));
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
    ReduceCheckerMinireduction() { reset(); }

    //! Reset minireduction to initial state
    void reset() {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::fill(reductions_[i].begin(), reductions_[i].end(), Value { });
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
    bool operator == (const ReduceCheckerMinireduction& other) const {
        // check dimensions
        if (num_buckets != other.num_buckets) return false;
        if (num_parallel != other.num_parallel) return false;
        // check all buckets for equality
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                if (reductions_[i][j] != other.reductions_[i][j]) return false;
            }
        }
        return true;
    }

    void all_reduce(api::Context& ctx) {
        reductions_ =
            ctx.net.AllReduce(reductions_,
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

    void update_bucket(const size_t idx, const size_t bucket, const Value& value) {
        reductions_[idx][bucket] = reduce(reductions_[idx][bucket], value);
    }

    std::array<reduction_t, num_parallel> reductions_;
    hash_fn hash_;
    ReduceFunction reduce;
};

} // namespace _detail

//! Whether to check reductions (when applicable)
static constexpr bool check_reductions_ = true;

//! Reduce checker - no-op for unsupported reduce functions
template <typename Key, typename Value, typename ReduceFunction, typename Enable = void>
class ReduceChecker : public noncopynonmove
{
public:
    template <typename K, typename V>
    void add_pre(const K&, const V&) { }

    template <typename KV>
    void add_pre(const KV&) { }

    template <typename K, typename V>
    void add_post(const K&, const V&) { }

    template <typename KV>
    void add_post(const KV&) { }

    bool check(api::Context&) { return true; }
};

//! Convenience dummy checker
using ReduceCheckerDummy = ReduceChecker<void, void, void>;

/*!
 * Reduce checker for supported reduce functions
 */
template <typename Key, typename Value, typename ReduceFunction>
class ReduceChecker<Key, Value, ReduceFunction,
                    typename std::enable_if_t<check_reductions_&&
                                              reduce_checkable_v<ReduceFunction> > >
    : public noncopynonmove
{
    using KeyValuePair = std::pair<Key, Value>;
    static constexpr bool debug = false;

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
        LOGC(debug && ctx.my_rank() == 0)
            << "check(): " << (success ? "yay" : "NAY");
        return success;
    }

private:
    _detail::ReduceCheckerMinireduction<Key, Value, ReduceFunction> mini_pre, mini_post;
};

//! Debug manipulators?
static constexpr bool debug = false;

//! Base class for reduce manipulators, using the Curiously Recurring Template
//! Pattern to provide the implementation of 'manipulate'
template <typename Strategy>
struct ReduceManipulatorBase : public ManipulatorBase {
    //! by default, manipulate all blocks (ranges)
    static const bool manipulate_only_once = true;

    //! Pair iterator key type
    template <typename It>
    using Key = typename std::iterator_traits<It>::value_type::first_type;

    //! Pair iterator value type
    template <typename It>
    using Value = typename std::iterator_traits<It>::value_type::second_type;

    //! No-op manipulator
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        return std::make_pair(begin, end);
    }

    //! Call operator, performing the manipulation.
    //! This wraps skipping empty keys and empty blocks (ranges)
    template <typename It>
    std::pair<It, It> operator () (It begin, It end) {
        if (Strategy::manipulate_only_once && made_changes()) {
            // abort
            return std::make_pair(begin, end);
        }

        It it = skip_empty_key(begin, end);
        std::pair<It, It> ret;
        if (it < end)
            ret = static_cast<Strategy*>(this)->manipulate(it, end);

        if (made_changes())
            return ret;
        else
            return std::make_pair(begin, end);
    }
};

//! Dummy No-Op Reduce Manipulator
struct ReduceManipulatorDummy : public ReduceManipulatorBase<ReduceManipulatorDummy> {};

//! Drops first element
struct ReduceManipulatorDropFirst
    : public ReduceManipulatorBase<ReduceManipulatorDropFirst>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        while (begin < end && (begin->first == Key<It>() ||
                               begin->second == Value<It>()))
            ++begin;
        if (begin < end) {
            sLOG << "Manipulating" << end - begin << "elements, dropping first";
            begin->first = Key<It>();
            begin->second = Value<It>();
            ++begin;
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorIncFirst
    : public ReduceManipulatorBase<ReduceManipulatorIncFirst>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing first";
        begin->second++;
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorRandFirst
    : public ReduceManipulatorBase<ReduceManipulatorRandFirst>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first value";
        begin->second = Value<It>{rng()};
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
private:
    std::mt19937 rng{std::random_device{}()};
};

//! Increments key of first element
struct ReduceManipulatorIncFirstKey
    : public ReduceManipulatorBase<ReduceManipulatorIncFirstKey>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing key of first";
        begin->first++;
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};


//! Increments value of first element
struct ReduceManipulatorRandFirstKey
    : public ReduceManipulatorBase<ReduceManipulatorRandFirstKey>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first key";
        begin->first = Key<It>{rng()};
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
private:
    std::mt19937 rng{std::random_device{}()};
};

//! Switches values of first and second element
struct ReduceManipulatorSwitchValues
    : public ReduceManipulatorBase<ReduceManipulatorSwitchValues>
{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        It next = skip_empty_key(begin + 1, end);
        if (next < end && *begin != *next) {
            sLOG << "Manipulating" << end - begin << "elements,"
                 << "switching values at pos 0 and" << next - begin;
            std::swap(begin->second, next->second);
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_REDUCE_HEADER

/******************************************************************************/
