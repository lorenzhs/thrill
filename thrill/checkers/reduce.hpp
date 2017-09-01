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
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/functional.hpp>
#include <thrill/checkers/manipulator.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

#include <array>
#include <cassert>
#include <random>
#include <utility>

namespace thrill {
namespace checkers {

template <typename hash_fn_, size_t bucket_bits_, size_t num_parallel_,
          size_t mod_range = (1ULL << (bucket_bits_ - 1))>
struct MinireductionConfig {
    using hash_fn = hash_fn_;
    static constexpr size_t bucket_bits = bucket_bits_;
    static constexpr size_t num_parallel = num_parallel_;
    static constexpr size_t mod_min = mod_range + 1;
    static constexpr size_t mod_max = 2 * mod_range;
    static_assert(mod_max <= (1ULL << bucket_bits),
                  "mod_max must fit into bucket_bits bits but doesn't");
};

template <typename Key>
using DefaultMinireductionConfig =
          MinireductionConfig<common::HashCrc32<Key>, 8, 4>;

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
    static constexpr size_t mod_min = Config::mod_min;
    static constexpr size_t mod_max = Config::mod_max;

    //! hash value type
    using hash_t = decltype(std::declval<hash_fn>()(std::declval<Key>()));
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

    // select smallest integer type that fits for transmission
    using transmit_t = select_uint_t<mod_max>;
    using transmit_reduction_t = std::array<transmit_t, num_buckets>;
    using transmit_table_t = std::array<transmit_reduction_t, num_parallel>;

    static constexpr bool debug = false;
    //! Enable extra debug output by setting this to true
    static constexpr bool extra_verbose = false;

public:
    ReduceCheckerMinireduction() { }

    //! Reset minireduction to initial state
    void reset(size_t seed) {
        // randomize the modulus
        std::mt19937 rng(seed);
        std::uniform_int_distribution<Value> dist(mod_min, mod_max);
        modulus_ = dist(rng);
        // communicate modulus to reduce function if supported
        if constexpr (reduce_modulo_builtin_v<ReduceFunction>) {
            reducefn.modulus = modulus_;
        }
        sLOG0 << "minireduction initialized with" << (const size_t)mod_min
              << "≤ modulus (" << modulus_ << ") ≤" << (const size_t)mod_max
              << "with bucket_bits =" << (const size_t)bucket_bits;
        // reset table to zero
        for (size_t i = 0; i < num_parallel; ++i) {
            std::fill(reductions_[i].begin(), reductions_[i].end(), Value { });
        }
    }

    //! Add a single item with Key key and Value value
    inline void push(const Key& key, const Value& value) {
        hash_t h = hash_(key);
        for (size_t idx = 0; idx < num_parallel; ++idx) {
            const size_t bucket = (h >> (idx * bucket_bits)) & bucket_mask;
            sLOGC(extra_verbose) << key << idx << bucket << "="
                                 << std::hex << bucket << h << std::dec;
            if constexpr(reduce_modulo_builtin_v<ReduceFunction>) {
                reductions_[idx][bucket] =
                    reducefn(reductions_[idx][bucket], value);
            } else {
                reductions_[idx][bucket] =
                    reducefn(reductions_[idx][bucket], value) % modulus_;
            }
        }
    }

    //! Compare local minireduction for equality
    bool operator == (const ReduceCheckerMinireduction& other) const {
        // check dimensions
        if (num_buckets != other.num_buckets) {
            sLOG << "bucket number mismatch:" << (size_t)num_buckets
                 << (size_t)other.num_buckets;
            return false;
        }
        if (num_parallel != other.num_parallel) {
            sLOG << "reduction number mismatch:" << (size_t)num_parallel
                 << (size_t)other.num_parallel;
            return false;
        }
        // check all buckets for equality
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                assert(reductions_[i][j] < modulus_);
                if (reductions_[i][j] != other.reductions_[i][j]) {
                    sLOG << "table entry mismatch at column" << i << "row" << j
                         << "values" << reductions_[i][j] << other.reductions_[i][j]
                         << "diff:" << reductions_[i][j] - other.reductions_[i][j];
                    return false;
                }
            }
        }
        return true;
    }

    void reduce(api::Context& ctx, size_t root = 0) {
        if (debug) {
            dump_to_log("Before");
        }

        // TODO: pack integers, don't just use smallest type that fits

        transmit_table_t transmit_table;
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                // Need to modulo manually, builtin modulo only means that it
                // does the modulo before the value would overflow!
                if constexpr(reduce_modulo_builtin_v<ReduceFunction>) {
                    transmit_table[i][j] =
                        static_cast<transmit_t>(reductions_[i][j] % modulus_);
                } else {
                    transmit_table[i][j] =
                        static_cast<transmit_t>(reductions_[i][j]);
                }
            }
        }

        // Add a modulo to the reduce function
        auto reducefn_mod = [&](const transmit_t &a, const transmit_t &b) {
            auto red = reducefn(static_cast<Value>(a), static_cast<Value>(b));
            return static_cast<transmit_t>(red % modulus_);
        };
        common::ComponentSum<transmit_table_t, decltype(reducefn_mod)>
            table_reduce(reducefn_mod);
        transmit_table = ctx.net.Reduce(transmit_table, root, table_reduce);

        if (ctx.net.my_rank() != root) return;

        // copy back into reductions_
        for (size_t i = 0; i < num_parallel; ++i) {
            for (size_t j = 0; j < num_buckets; ++j) {
                reductions_[i][j] = static_cast<Value>(transmit_table[i][j]);
            }
        }

        if (debug && ctx.net.my_rank() == root) {
            dump_to_log("Run");
        }
    }

protected:
    void dump_to_log(const std::string& name = "Run") {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::stringstream s;
            s << name << " " << i << ", mod " << modulus_ << ": ";
            for (size_t j = 0; j < num_buckets; ++j) {
                s << reductions_[i][j] << " ";
            }
            LOG1 << s.str();
        }
    }

    table_t reductions_;
    Value modulus_;
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
    ReduceChecker(size_t seed = 0) : rng(seed), have_checked(false),
                                     cached_result(false) { }

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
        // It's important that we seed both minireductions with the same seed
        auto seed = rng();
        mini_pre.reset(seed);
        mini_post.reset(seed);
        have_checked = false;
        cached_result = false;
    }

    //! Do the check. Result is only meaningful at root (PE 0), and cached.
    //! Cache is invalidated only upon reset().
    bool check(api::Context& ctx) {
        if (have_checked) { // return cached result
            return cached_result;
        }

        mini_pre.reduce(ctx);
        if constexpr (debug) ctx.net.Barrier();  // for logging
        mini_post.reduce(ctx);

        bool result = true;            // return true on non-root PEs
        if (ctx.my_rank() == 0) {
            result = (mini_pre == mini_post);
        }

        have_checked = true;
        LOGC(debug && ctx.my_rank() == 0)
            << "check(): " << (result ? "yay" : "NAY");
        cached_result = result;
        return result;
    }

private:
    std::mt19937 rng;
    Minireduction mini_pre, mini_post;
    bool have_checked, cached_result;
};

//! Debug manipulators?
static constexpr bool debug = false;

template <typename Key_Ex, typename Key_Eq, typename RMTI_>
struct ReduceManipulatorConfig {
    using KeyEx = Key_Ex;
    using KeyEq = Key_Eq;
    using RMTI = RMTI_;

    using Key = typename common::FunctionTraits<KeyEx>::result_type;
    using Value = typename RMTI::Value;
    using TableItem = typename RMTI::TableItem;

    ReduceManipulatorConfig(const KeyEx& key_ex_, const KeyEq& key_eq_)
        : key_ex(key_ex_), key_eq(key_eq_) { }

    auto GetKey(const TableItem& t) const {
        return RMTI::GetKey(t, key_ex);
    }

    bool IsDefaultKey(const TableItem& t) const {
        return key_eq(GetKey(t), Key());
    }

    //! Extract and Equality check in one
    bool key_exq(const TableItem& v1, const TableItem& v2) const {
        return key_eq(GetKey(v1), GetKey(v2));
    }

    const KeyEx key_ex;
    const KeyEq key_eq;
};

//! Base class for reduce manipulators, using the Curiously Recurring Template
//! Pattern to provide the implementation of 'manipulate'
template <typename Strategy>
struct ReduceManipulatorBase : public ManipulatorBase {
    //! by default, manipulate all blocks (ranges)
    static const bool manipulate_only_once = true;

    //! Skip all items whose key is the default
    template <typename It, typename Config>
    It skip_empty_key(It begin, It end, Config config) const {
        while (begin < end && config.IsDefaultKey(*begin)) ++begin;
        return begin;
    }

    //! Skip all items whose key is the default or equal to begin's key
    template <typename It, typename Config>
    It skip_to_next_key(It begin, It end, Config config) const {
        It next = begin + 1;
        while (next < end && (config.IsDefaultKey(*next) ||
                              config.key_exq(*next, *begin)))
            ++next;
        return next;
    }

    template <typename It, typename Config>
    std::vector<It> get_distinct_keys(It begin, It end, size_t n, Config config) const {
        std::vector<It> result(n);
        result[0] = skip_to_next_key(begin, end, config);

        auto is_first_occurrence = [&config, &result](size_t idx) {
            for (size_t pos = 0; pos < idx; ++pos) {
                if (config.key_exq(*result[pos], *result[idx]))
                    return false;
            }
            return true;
        };

        for (size_t i = 1; i < n; ++i) {
            result[i] = result[i - 1];
            do {
                result[i] = skip_to_next_key(result[i], end, config);
            } while (result[i] < end && !is_first_occurrence(i));
        }

        return result;
    }

    //! No-op manipulator
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config /* config */) {
        return std::make_pair(begin, end);
    }

    //! Call operator, performing the manipulation.
    //! This wraps skipping empty keys and empty blocks (ranges)
    template <typename It, typename Config>
    std::pair<It, It> operator () (It begin, It end, Config config) {
        if (Strategy::manipulate_only_once && made_changes()) {
            // abort
            return std::make_pair(begin, end);
        }

        It it = skip_empty_key(begin, end, config);
        std::pair<It, It> ret;
        if (it < end) {
            ret = static_cast<Strategy*>(this)->manipulate(it, end, config);
        }

        if (made_changes()) {
            return ret;
        }
        else {
            return std::make_pair(begin, end);
        }
    }
};

//! Dummy No-Op Reduce Manipulator
struct ReduceManipulatorDummy : public ReduceManipulatorBase<ReduceManipulatorDummy>{ };

//! Drops first element
struct ReduceManipulatorDropFirst
    : public ReduceManipulatorBase<ReduceManipulatorDropFirst>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config config) {
        while (begin < end && (config.IsDefaultKey(*begin) ||
                               begin->second == typename Config::Value())) {
            ++begin;
        }
        if (begin < end) {
            sLOG << "Manipulating" << end - begin << "elements, dropping first:"
                 << maybe_print(*begin);
            begin->first = typename Config::Key();
            begin->second = typename Config::Value();
            ++begin;
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorIncFirst
    : public ReduceManipulatorBase<ReduceManipulatorIncFirst>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing first:" << maybe_print(*begin);
        begin->second++;
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Of the first `2n` elements with distinct keys, increments value of first `n`
//! elements and decrements that of next `n`
template <size_t n = 1>
struct ReduceManipulatorIncDec
    : public ReduceManipulatorBase<ReduceManipulatorIncDec<n> >{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config config) {
        auto arr = this->get_distinct_keys(begin, end, 2 * n, config);

        if (arr.back() < end) {
            sLOG << "Manipulating" << end - begin
                 << "elements, incrementing first" << n
                 << "and decrementing second" << n << "of" << maybe_print(arr);
            for (size_t i = 0; i < n; ++i) {
                arr[i]->second++;
                arr[n + i]->second--;
            }
            this->made_changes_ = true;  // this-> required for unknown reasons
        }
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorRandFirst
    : public ReduceManipulatorBase<ReduceManipulatorRandFirst>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first value:" << maybe_print(*begin);
        typename Config::Value old = begin->second;
        do {
            begin->second = static_cast<typename Config::Value>(rng());
        } while (old == begin->second);
        sLOG << "Update: old val" << maybe_print(old)
             << "new" << maybe_print(begin->second);
        made_changes_ = true;
        return std::make_pair(begin, end);
    }

private:
    std::mt19937 rng { std::random_device { } () };
};

//! Increments key of first element
struct ReduceManipulatorIncFirstKey
    : public ReduceManipulatorBase<ReduceManipulatorIncFirstKey>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing key of first:" << maybe_print(*begin);
        begin->first++; // XXX
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorRandFirstKey
    : public ReduceManipulatorBase<ReduceManipulatorRandFirstKey>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config config) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing first key" << maybe_print(*begin);
        auto old_key = config.GetKey(*begin);
        do {
            begin->first = static_cast<typename Config::Key>(rng()); // XXX
        } while (config.key_eq(old_key, config.GetKey(*begin)));
        sLOG << "Update: old key" << maybe_print(old_key)
             << "new" << maybe_print(begin->first);
        made_changes_ = true;
        return std::make_pair(begin, end);
    }

private:
    std::mt19937 rng { std::random_device { } () };
};

//! Switches values of first and second element
struct ReduceManipulatorSwitchValues
    : public ReduceManipulatorBase<ReduceManipulatorSwitchValues>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, Config config) {
        It next = skip_empty_key(begin + 1, end, config);
        if (next < end && *begin != *next) {
            sLOG << "Manipulating" << end - begin << "elements,"
                 << "switching values at pos 0 and" << next - begin
                 << maybe_print(*begin) << maybe_print(*next);
            std::swap(begin->second, next->second);
            made_changes_ = true;
        }
        return std::make_pair(begin, end);
    }
};

using DummyReduceDriver = Driver<ReduceCheckerDummy, ReduceManipulatorDummy>;

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_REDUCE_HEADER

/******************************************************************************/
