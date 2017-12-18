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
#include <tlx/meta/log2.hpp>

#include <array>
#include <cassert>
#include <random>
#include <utility>

// for std::is_detected
#include <experimental/type_traits>

namespace thrill {
namespace checkers {

template <typename hash_fn_, size_t num_buckets_, size_t num_parallel_,
          size_t mod_range = 32768 /* XXX TODO */>
struct MinireductionConfig {
    //! type of hash function to use (e.g. CRC-32C or tabulation hashing)
    using hash_fn = hash_fn_;
    //! number of buckets
    static constexpr size_t num_buckets = num_buckets_;
    //! is the number of buckets a power of two?
    static constexpr bool   pow2_buckets = is_power_of_two(num_buckets_);
    //! ceil(log2(num_buckets))
    static constexpr size_t log2_buckets = tlx::Log2<num_buckets_>::ceil;
    //! number of minireduction instances to execute in parallel
    static constexpr size_t num_parallel = num_parallel_;
    //! minimum value for the minireduction's modulus
    static constexpr size_t mod_min = mod_range + 1;
    //! maximum value for the minireduction's modulus
    static constexpr size_t mod_max = 2 * mod_range;
    //! Expected failure rate delta for this configuration
    static constexpr double exp_delta = pow_helper<num_parallel>::pow(
        1.0/num_buckets + 1.0/mod_min);
};

//! A default configuration with 4 instances, 256 buckets and CRC-32C hashing
template <typename Key>
using DefaultMinireductionConfig =
          MinireductionConfig<common::HashCrc32<Key>, 8, 4>;

namespace _detail {
//! Reduce checker minireduction, this is where the magic happens
template <typename Key, typename Value, typename ReduceFunction,
          typename Config = DefaultMinireductionConfig<Key> >
class ReduceCheckerMinireduction : public noncopynonmove
{
    static_assert(reduce_checkable_v<ReduceFunction>,
                  "Reduce function isn't (marked) checkable");

    // Get stuff from the config
    using hash_fn = typename Config::hash_fn;
    //! Number of buckets
    static constexpr size_t num_buckets = Config::num_buckets;
    //! Number of parallel instances
    static constexpr size_t num_parallel = Config::num_parallel;

    //! Hash function value type
    using hash_t = decltype(std::declval<hash_fn>()(std::declval<Key>()));
    //! Number of bits in the hash function's output
    static constexpr size_t hash_bits = 8 * sizeof(hash_t);
    //! Check that hash function produces enough data
    static_assert(num_parallel * Config::log2_buckets <= hash_bits,
                  "hash_fn bits insufficient for requested number of buckets");

    //! Hash bits per instance.  If num_buckets is a power of two, simply crop
    //! out the correct number of bits.  Otherwise, use as many bits as possible
    //! to maximise uniformity of the scaling bucket assignment
    static constexpr size_t hash_shift = (hash_bits / num_parallel);
    //! Mask to cut down hash values to required number of bits
    static constexpr size_t bucket_mask = if_v<Config::pow2_buckets,
                                               (1ULL << Config::log2_buckets) - 1,
                                               (1ULL << hash_shift) - 1>;
    //! Scale factor for non-power-of-two num_buckets to avoid expensive modulo
    //! computations (divisions) in bucket assignment
    static constexpr double scale_factor =
        static_cast<double>(num_buckets) /
        static_cast<double>(1ULL << hash_shift);
    static_assert(Config::pow2_buckets || scale_factor < 1,
                  "insufficient number of bits in hash function output");

    //! data type for a single minireduction's state
    using reduction_t = std::array<Value, num_buckets>;
    //! data type for a number of parallel minireductions
    using table_t = std::array<reduction_t, num_parallel>;
    //! data type for moduli
    using modulus_t = std::array<Value, num_parallel>;
    //! data type for reduce function array
    using reducefn_t = std::array<ReduceFunction, num_parallel>;

    //! smallest integer type that fits a minireduction value for transmission
    //! (modulo some value between Config::mod_min and Config::mod_max)
    using transmit_t = select_uint_t<Config::mod_max>;
    //! data type for a single minireduction's state for transmission
    using transmit_reduction_t = std::array<transmit_t, num_buckets>;
    //! data type for a number of parallel minireductions for transmission
    using transmit_table_t = std::array<transmit_reduction_t, num_parallel>;

    static constexpr bool debug = false;
    //! Enable extra debug output by setting this to true
    static constexpr bool extra_verbose = false;

public:
    ReduceCheckerMinireduction() { }

    //! Reset minireduction to initial state
    void reset(size_t seed) {
        std::mt19937 rng(seed);

        // Randomize hash function if supported
        if constexpr(std::experimental::is_detected_v<has_init_t, hash_fn>) {
            LOG << "initializing hash function with seed...";
            hash_.init(rng());
        } else {
            LOG << "hash function does not support .init(const size_t seed)";
        }

        // Randomize the modulus
        std::uniform_int_distribution<Value>
            dist(Config::mod_min, Config::mod_max);
        for (size_t i = 0; i < num_parallel; ++i) {
            modulus_[i] = dist(rng);
            // communicate modulus to reduce function if supported
            if constexpr (reduce_modulo_builtin_v<ReduceFunction>) {
                reducefn[i].modulus = modulus_[i];
            }
            LOG0 << "modulus " << i << " = " << modulus_[i];
        }
        sLOG0 << "minireduction initialized with" << (size_t)Config::mod_min
              << "≤ modulus ≤" << (size_t)Config::mod_max
              << "with" << (const size_t)num_buckets << "buckets";
        // reset table to zero
        for (size_t i = 0; i < num_parallel; ++i) {
            std::fill(reductions_[i].begin(), reductions_[i].end(), Value { });
        }
    }

    //! Add a single item with Key key and Value value
    inline void push(const Key& key, const Value& value) {
        hash_t h = hash_(key);
        for (size_t idx = 0; idx < num_parallel; ++idx) {
            size_t bucket = (h >> (idx * hash_shift)) & bucket_mask;
            if constexpr (!Config::pow2_buckets) {
                // scale hash value to 0..num_buckets - 1
                bucket = static_cast<size_t>(
                    static_cast<double>(bucket) * scale_factor);
                assert(0 <= bucket && bucket < num_buckets);
            }
            sLOGC(extra_verbose) << key << idx << bucket << "="
                                 << std::hex << bucket << h << std::dec;
            if constexpr (reduce_modulo_builtin_v<ReduceFunction>) {
                reductions_[idx][bucket] =
                    reducefn[idx](reductions_[idx][bucket], value);
            } else {
                reductions_[idx][bucket] =
                    reducefn[0](reductions_[idx][bucket], value) % modulus_[idx];
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
                assert(reductions_[i][j] < modulus_[i]);
                if (reductions_[i][j] != other.reductions_[i][j]) {
                    sLOG << "table entry mismatch at column" << i << "row" << j
                         << "values" << reductions_[i][j] << other.reductions_[i][j]
                         << "diff:"
                         << std::min(reductions_[i][j] - other.reductions_[i][j],
                                     other.reductions_[i][j] - reductions_[i][j]);
                    return false;
                }
            }
        }
        return true;
    }

    //! Simple reduction to reduce minireduction.  Output is in reductions_ at
    //! worker with rank `root`.
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
                if constexpr (reduce_modulo_builtin_v<ReduceFunction>) {
                    transmit_table[i][j] =
                        static_cast<transmit_t>(reductions_[i][j] % modulus_[i]);
                } else {
                    transmit_table[i][j] =
                        static_cast<transmit_t>(reductions_[i][j]);
                }
            }
        }

        // Add a modulo to the reduce function
        auto reducefn_mod = [&](const transmit_table_t &a,
                                const transmit_table_t &b) {
            transmit_table_t out;
            for (size_t i = 0; i < num_parallel; ++i) {
                for (size_t j = 0; j < num_buckets; ++j) {
                    out[i][j] = static_cast<transmit_t>(
                        reducefn[i](static_cast<Value>(a[i][j]),
                                    static_cast<Value>(b[i][j]))
                        % modulus_[i]);
                }
            }
            return out;
        };
        transmit_table = ctx.net.Reduce(transmit_table, root, reducefn_mod);

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
    //! dump internal state of the minireduction to the log
    void dump_to_log(const std::string& name = "Run") {
        for (size_t i = 0; i < num_parallel; ++i) {
            std::stringstream s;
            s << name << " " << i << ", mod " << modulus_[i] << ": ";
            for (size_t j = 0; j < num_buckets; ++j) {
                s << reductions_[i][j] << " ";
            }
            LOG1 << s.str();
        }
    }

    table_t reductions_;
    modulus_t modulus_;
    hash_fn hash_;
    reducefn_t reducefn;
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
    void add_pre(const Key& key, const KeyValuePair& kv) {
        mini_pre.push(key, kv.second);
    }
    void add_pre(const KeyValuePair& kv) {
        mini_pre.push(kv.first, kv.second);
    }

    void add_post(const Key& key, const Value& value) {
        mini_post.push(key, value);
    }
    void add_post(const Key& key, const KeyValuePair& kv) {
        mini_post.push(key, kv.second);
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

    template <typename V = Value, typename = int>
    static auto GetVal(const TableItem& t) {
        return t;
    }

    template <typename V = Value, decltype((void)V::second_type, 0)>
    static auto GetVal(const TableItem& t) {
        return t->second;
    }

    bool IsDefaultKey(const TableItem& t) const {
        return key_eq(GetKey(t), Key());
    }

    template <typename V = decltype(GetVal<Value>(std::declval<TableItem>()))>
    typename std::enable_if_t<!has_equal<V,V>::value ||
                              !std::is_default_constructible_v<V>, bool>
    IsDefaultVal(const TableItem& /*t*/) const {
        // If no operator== or default constructor exist, just return false
        return false;
    }

    template <typename V = decltype(GetVal<Value>(std::declval<TableItem>()))>
    typename std::enable_if_t<has_equal<V,V>::value &&
                              std::is_default_constructible_v<V>, bool>
    IsDefaultVal(const TableItem& t) const {
        return GetVal(t) == V{};
    }

    bool IsEitherDefault(const TableItem& t) const {
        return IsDefaultKey(t) || IsDefaultVal(t);
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
    //! by default, manipulate only one block (range)
    static const bool manipulate_only_once = true;

    //! Skip all items whose key is the default
    template <typename It, typename Config>
    It skip_empty_key(It begin, It end, Config config, int step = 1) const {
        while (begin != end && config.IsDefaultKey(*begin)) { begin += step; }
        return begin;
    }

    //! Find an element whose key isn't the default, behind the element if
    //! possible, otherwise before it.  Returns end if none found.
    template <typename It, typename Config>
    It find_nonempty_elem(It begin, It elem, It end, Config config) const {
        assert(begin <= elem && elem <= end);

        It it = elem;
        while (it < end && config.IsEitherDefault(*it)) { ++it; }
        if (it < end) { return it; }

        it = elem - 1;
        while (it >= begin && config.IsEitherDefault(*it)) { --it; }
        if (it >= begin) { return it; }

        return end;
    }

    //! Skip all items whose key is the default or equal to begin's key
    template <typename It, typename Config>
    It skip_to_next_key(It begin, It end, Config config) const {
        It next = begin + 1;
        while (next < end && (config.IsEitherDefault(*next) ||
                              config.key_exq(*next, *begin)))
            ++next;
        return next;
    }

    template <typename It, typename Config>
    It choose_random_nonempty_elem(It begin, It end, Config config) {
        It elem = begin + (rng() % (end - begin));
        return this->find_nonempty_elem(begin, elem, end, config);
    }

    template <typename It, typename Config>
    std::vector<It> get_distinct_keys(It begin, It end, size_t n, Config config) {
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

        std::shuffle(result.begin(), result.end(), rng);

        return result;
    }

    //! No-op manipulator
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It /* random */,
                                 Config /* config */) {
        return std::make_pair(begin, end);
    }

    //! Call operator, performing the manipulation.
    //! This wraps skipping empty keys and empty blocks (ranges)
    template <typename It, typename Config>
    std::pair<It, It> operator () (It begin, It end, Config config,
                                   size_t partition_id, size_t num_partitions) {
        if (Strategy::manipulate_only_once) {
            if (made_changes()) {
                // abort
                return std::make_pair(begin, end);
            } else if (target_partition == (size_t)-1) {
                // choose a random partition to manipulate
                // XXX replace 0 with partition_id?
                std::uniform_int_distribution<size_t> dist(0, num_partitions - 1);
                target_partition = dist(rng);
                LOG << "Choosing partition " << target_partition << " of " << num_partitions;
            }
            assert(0 <= target_partition && target_partition < num_partitions);

            if (partition_id != target_partition) {
                // abort, wrong partition
                return std::make_pair(begin, end);
            }
        }

        LOG << "Manipulating partition " << partition_id;
        It it = skip_empty_key(begin, end, config);
        It random = choose_random_nonempty_elem(begin, end, config);
        std::pair<It, It> ret;
        if (it < end && random < end) {
            ret = static_cast<Strategy*>(this)->manipulate(it, end, random, config);
        }

        if (made_changes()) {
            return ret;
        }
        else if (Strategy::manipulate_only_once &&
                 partition_id + 1 < num_partitions) {
            target_partition++; // try again next time
        }

        return std::make_pair(begin, end);
    }

protected:
    std::mt19937 rng { std::random_device{}() };
    size_t target_partition = -1;
};

//! Dummy No-Op Reduce Manipulator
struct ReduceManipulatorDummy : public ReduceManipulatorBase<ReduceManipulatorDummy>{ };

//! Flip a random bit somewhere in a random element (key or value)
struct ReduceManipulatorBitflip
    : public ReduceManipulatorBase<ReduceManipulatorBitflip>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config /* config */) {
        It elem = random;

        auto old = *elem; // for debug logging
        auto rand = this->rng();
        // manipulate key or value?
        if (rand & 0x1) {
            if constexpr (std::is_same_v<std::string, typename Config::Key>) {
                // mess up a string
                size_t size = elem->first.size();
                assert(size > 0);
                size_t bit = (rand >> 1) & 7;
                size_t pos = (rand >> 4) % size;
                elem->first[pos] ^= (1ULL << bit);
                sLOG << "Manipulating" << end - begin << "elements,"
                     << "flipping bit" << bit << "in char" << pos
                     << "in (string) key of #" << elem - begin
                     << maybe_print(old) << "→" << maybe_print(*elem);
            } else {
                constexpr size_t key_bits = 8 * sizeof(typename Config::Key);
                size_t bit = (rand >> 1) & (key_bits - 1);
                elem->first ^= (1ULL << bit);
                sLOG << "Manipulating" << end - begin << "elements,"
                     << "flipping bit" << bit << "in key of #" << elem - begin
                     << maybe_print(old) << "→" << maybe_print(*elem);
            }
        } else {
            constexpr size_t val_bits = 8 * sizeof(typename Config::Value);
            size_t bit = (rand >> 1) & (val_bits - 1);
            elem->second ^= (1ULL << bit);
            sLOG << "Manipulating" << end - begin << "elements,"
                 << "flipping bit" << bit << "in value of #" << elem - begin
                 << maybe_print(old) << "→" << maybe_print(*elem);
        }
        assert(old != *elem);
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Drops some element
struct ReduceManipulatorDrop
    : public ReduceManipulatorBase<ReduceManipulatorDrop>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config /* config */) {
        sLOG << "Manipulating" << end - begin << "elements, dropping no"
             << random - begin << ": " << maybe_print(*random);
        random->first = typename Config::Key();
        random->second = decltype(random->second){};
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Increments value of some element
struct ReduceManipulatorInc
    : public ReduceManipulatorBase<ReduceManipulatorInc>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing no" << random - begin << ":"
             << maybe_print(*random);
        random->second++;
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
    std::pair<It, It> manipulate(It begin, It end, It /* random */, Config config) {
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

//! Increments value of some element
struct ReduceManipulatorRandomize
    : public ReduceManipulatorBase<ReduceManipulatorRandomize>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing value of no" << random - begin << ":"
             << maybe_print(*random);
        auto old = random->second;
        do {
            random->second = static_cast<decltype(random->second)>(this->rng());
        } while (old == random->second);
        sLOG << "Update: old val" << maybe_print(old)
             << "new" << maybe_print(random->second);
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Increments key of some element
struct ReduceManipulatorIncKey
    : public ReduceManipulatorBase<ReduceManipulatorIncKey>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config /* config */) {
        sLOG << "Manipulating" << end - begin
             << "elements, incrementing key of no" << random - begin << ":"
             << maybe_print(*random);

        if constexpr (std::is_same_v<std::string, typename Config::Key>) {
            random->first += "1";
        } else {
            random->first++;
        }
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Increments value of first element
struct ReduceManipulatorRandKey
    : public ReduceManipulatorBase<ReduceManipulatorRandKey>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config config) {
        sLOG << "Manipulating" << end - begin
             << "elements, randomizing key of no" << random - begin << ":"
             << maybe_print(*begin);
        auto old_key = config.GetKey(*random);
        do {
            if constexpr (std::is_same_v<std::string, typename Config::Key>) {
                random->first = "a"; // chosen by fair dice roll, very random lol
            } else {
                random->first = static_cast<typename Config::Key>(this->rng());
            }
        } while (config.key_eq(old_key, config.GetKey(*random)));
        sLOG << "Update: old key" << maybe_print(old_key)
             << "new" << maybe_print(random->first);
        made_changes_ = true;
        return std::make_pair(begin, end);
    }
};

//! Switches values of some two elements
struct ReduceManipulatorSwitchValues
    : public ReduceManipulatorBase<ReduceManipulatorSwitchValues>{
    template <typename It, typename Config>
    std::pair<It, It> manipulate(It begin, It end, It random, Config config) {
        It a = random, b = skip_to_next_key(random, end, config);
        while (b != end && (config.GetKey(*b) == config.GetKey(*a) ||
                            b->second == a->second)) {
            ++b;
        }
        // if no success, look from the front
        if (b == end) b = skip_to_next_key(begin, random, config);
        while (b < random && (config.GetKey(*b) == config.GetKey(*a) ||
                              b->second == a->second)) {
            ++b;
        }
        if (b != random && b < end) {
            assert(a->second != b->second);
            sLOG << "Manipulating" << end - begin << "elements,"
                 << "switching values of" << maybe_print(*a) << "and"
                 << maybe_print(*b);
            std::swap(a->second, b->second);
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
