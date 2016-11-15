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

template <typename hash_fn_, size_t bucket_bits_, size_t num_parallel_>
struct MinireductionConfig {
    using hash_fn = hash_fn_;
    static constexpr size_t bucket_bits = bucket_bits_;
    static constexpr size_t num_parallel = num_parallel_;
};

template <typename Key>
using DefaultMinireductionConfig =
    MinireductionConfig<common::hash_crc32<Key>, 8, 4>;

namespace _detail {
//! Reduce checker minireduction helper
template <typename Key, typename Value, typename ReduceFunction,
          typename Config = DefaultMinireductionConfig<Key> >
class ReduceCheckerMinireduction : public noncopynonmove
{
    static_assert(reduce_checkable_v<ReduceFunction>,
                  "Reduce function isn't (marked) checkable");

    using KeyValuePair = std::pair<Key, Value>;

    // Get stuff from the config
    using hash_fn = typename Config::hash_fn;
    static constexpr size_t bucket_bits = Config::bucket_bits;
    static constexpr size_t num_parallel = Config::num_parallel;

    //! hash value type
    using hash_t = decltype(hash_fn()(Key { }));
    //! Check that hash function produces enough data
    static_assert(bucket_bits <= 8 * sizeof(hash_t),
                  "hash_fn produces fewer bits than needed to discern buckets");
    static_assert(num_parallel * bucket_bits <= 8 * sizeof(hash_t),
                  "hash_fn bits insufficient for requested number of buckets");
    //! Number of buckets
    static constexpr size_t num_buckets = 1ULL << bucket_bits;
    //! Mask to extract a bucket
    static constexpr size_t bucket_mask = (1ULL << bucket_bits) - 1;

    using reduction_t = std::array<Value, num_buckets>;
    using table_t = std::array<reduction_t, num_parallel>;

    static constexpr bool debug = false;
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
            const size_t bucket = (h >> (idx * bucket_bits)) & bucket_mask;
            reductions_[idx][bucket] =
                reducefn(reductions_[idx][bucket], value);
        }
    }

    //! Compare local minireduction for equality
    bool operator == (const ReduceCheckerMinireduction& other) const {
        // check dimensions
        if (num_buckets != other.num_buckets) {
            sLOG << "bucket number mismatch:" << num_buckets
                 << other.num_buckets;
            return false;
        }
        if (num_parallel != other.num_parallel) {
            sLOG << "reduction number mismatch:" << num_parallel
                 << other.num_parallel;
            return false;
        }
        // check all buckets for equality
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                if (reductions_[i][j] != other.reductions_[i][j]) {
                    sLOG << "table entry mismatch at column" << i << "row" << j
                         << "values" << reductions_[i][j]
                         << other.reductions_[i][j];
                    return false;
                }
            }
        }
        return true;
    }

    void reduce(api::Context& ctx, size_t root = 0) {
        if (extra_verbose && ctx.net.my_rank() == root) {
            dump_to_log("Before");
        }

        auto table_reduce =
            common::ComponentSum<table_t, ReduceFunction>(reducefn);
        reductions_ = ctx.net.AllReduce(reductions_, table_reduce);

        if (extra_verbose && ctx.net.my_rank() == root) {
            dump_to_log("Run");
        }
    }

protected:
    void dump_to_log(const std::string& name = "Run") {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::stringstream s;
            s << name << " " << i << ": ";
            for (size_t j = 0; j < num_buckets; ++j) {
                s << reductions_[i][j] << " ";
            }
            LOG1 << s.str();
        }
    }

    table_t reductions_;
    hash_fn hash_;
    ReduceFunction reducefn;
};

} // namespace _detail

//! Whether to check reductions (when applicable)
static constexpr bool check_reductions_ = true;

//! Reduce checker - no-op for unsupported reduce functions
template <typename Key, typename Value, typename ReduceFunction,
          typename Config = DefaultMinireductionConfig<Key>,
          typename Enable = void>
class ReduceChecker : public noncopynonmove
{
public:
    template <typename K, typename V>
    void add_pre(const K& /*unused*/, const V& /*unused*/) { }

    template <typename KV>
    void add_pre(const KV& /*unused*/) { }

    template <typename K, typename V>
    void add_post(const K& /*unused*/, const V& /*unused*/) { }

    template <typename KV>
    void add_post(const KV& /*unused*/) { }

    bool check(api::Context& /*unused*/) { return true; }

    void reset() { }
};

//! Convenience dummy checker
using ReduceCheckerDummy = ReduceChecker<void, void, std::hash<void>, void>;

/*!
 * Reduce checker for supported reduce functions
 */
template <typename Key, typename Value, typename ReduceFunction, typename Config>
class ReduceChecker<Key, Value, ReduceFunction, Config,
                    typename std::enable_if_t<check_reductions_&&
                                              reduce_checkable_v<ReduceFunction> > >
    : public noncopynonmove
{
    using KeyValuePair = std::pair<Key, Value>;
    using Minireduction = _detail::ReduceCheckerMinireduction<
        Key, Value, ReduceFunction, Config>;
    static constexpr bool debug = false;

public:
    ReduceChecker() : have_checked(false), result(false) { }

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

    void reset() {
        mini_pre.reset();
        mini_post.reset();
        have_checked = false;
        result = false;
    }

    //! Do the check. Result is only meaningful at root (PE 0)
    bool check(api::Context& ctx) {
        if (have_checked) {
            return result;
        }

        have_checked = true;
        mini_pre.reduce(ctx);
        mini_post.reduce(ctx);
        if (ctx.my_rank() != 0) { return true; }// no point in checking

        result = (mini_pre == mini_post);
        LOGC(debug && ctx.my_rank() == 0)
            << "check(): " << (result ? "yay" : "NAY");
        return result;
    }

private:
    Minireduction mini_pre, mini_post;
    bool have_checked, result;
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
        if (it < end) {
            ret = static_cast<Strategy*>(this)->manipulate(it, end);
        }

        if (made_changes()) {
            return ret;
        } else {
            return std::make_pair(begin, end);
        }
    }
};

//! Dummy No-Op Reduce Manipulator
struct ReduceManipulatorDummy : public ReduceManipulatorBase<ReduceManipulatorDummy>{ };

//! Drops first element
struct ReduceManipulatorDropFirst
    : public ReduceManipulatorBase<ReduceManipulatorDropFirst>{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        while (begin < end && (begin->first == Key<It>() ||
                               begin->second == Value<It>())) {
            ++begin;
        }
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
    : public ReduceManipulatorBase<ReduceManipulatorIncFirst>{
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
    : public ReduceManipulatorBase<ReduceManipulatorRandFirst>{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first value";
        begin->second = Value<It>{ rng() };
        made_changes_ = true;
        return std::make_pair(begin, end);
    }

private:
    std::mt19937 rng { std::random_device { } () };
};

//! Increments key of first element
struct ReduceManipulatorIncFirstKey
    : public ReduceManipulatorBase<ReduceManipulatorIncFirstKey>{
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
    : public ReduceManipulatorBase<ReduceManipulatorRandFirstKey>{
    template <typename It>
    std::pair<It, It> manipulate(It begin, It end) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first key";
        begin->first = Key<It>{ rng() };
        made_changes_ = true;
        return std::make_pair(begin, end);
    }

private:
    std::mt19937 rng { std::random_device { } () };
};

//! Switches values of first and second element
struct ReduceManipulatorSwitchValues
    : public ReduceManipulatorBase<ReduceManipulatorSwitchValues>{
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
