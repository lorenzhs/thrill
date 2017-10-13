/*******************************************************************************
 * examples/checkers/word_count.hpp
 *
 * Not a real header, just factored out commons for wordcount.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_CHECKERS_WORD_COUNT_HEADER
#define THRILL_EXAMPLES_CHECKERS_WORD_COUNT_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/reduce.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string_view.hpp>

#include "../word_count/random_text_writer.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <random>

using namespace thrill; // NOLINT

thread_local int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

constexpr int loop_fct = 50;
constexpr int warmup_its = 5;

// to subtract traffic RX/TX pairs
template <typename T, typename U>
std::pair<T, U> operator - (const std::pair<T, U>& a, const std::pair<T, U>& b) {
    return std::make_pair(a.first - b.first, a.second - b.second);
}

template <typename fptype = float>
class zipf_generator {
    std::vector<fptype> dist;
    std::uniform_real_distribution<fptype> rng;
    std::mt19937 gen;
    size_t num;
    double s;

public:
    zipf_generator(const size_t seed, const size_t num_, const double s_)
        : dist(num_ + 1), rng(0.0, 1.0), gen(seed), num(num_ + 1), s(s_)
    {
        // precompute distribution
        dist[0] = 0.0;

        double curr = 0.0;
        for (size_t i = 1; i < num; ++i) {
            curr += pow(static_cast<double>(i), -s);
            dist[i] = static_cast<fptype>(curr);
        }

        // normalize entries
        for (size_t i = 1; i < num; ++i) {
            dist[i] /= static_cast<fptype>(curr);
        }
    }

    zipf_generator(const zipf_generator &other) = delete;
    zipf_generator(zipf_generator &&other) = delete;
    zipf_generator &operator=(const zipf_generator &other) = delete;
    zipf_generator &&operator=(zipf_generator &&other) = delete;

    size_t next() {
        fptype random = rng(gen); // uniform
        // magic parameter tuning result, see below for reasoning
        if (num >= 250000) {
            return std::upper_bound(dist.begin(), dist.end(), random) - dist.begin() - 1;
        } else {
             // This version is faster for small values of num, especially when using g++
             // As the distribution is strongly skewed towards the low end, branch prediction
             // becomes cheaper than a conditional move for more than around 250k elements.
             size_t low(0), high(num), mid, half;
             while ((half = (high / 2)) > 0) {
                 mid = low + half;
                 low = (dist[mid] > random) ? low : mid;
                 high -= half;
             }
             return low;
         }
    }
};

auto word_count = [](
    const auto& manipulator, const auto& config,
    const std::string& manip_name,
    const std::string& config_name,
    const size_t words_per_worker, const size_t distinct_words,
    const size_t seed, const int reps)
{
    using Key = uint64_t;
    using Value = uint64_t;
    using WordCountPair = std::pair<Key, Value>;
    // checked_plus is important for making modulo efficient
    using ReduceFn = checkers::checked_plus<Value>;

    using Config = std::decay_t<decltype(config)>;
    using Checker = checkers::ReduceChecker<Key, Value, ReduceFn, Config>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();
    std::mt19937 rng(true_seed);
    zipf_generator<double> zipf(rng(), distinct_words, 1.0);
    auto generator = [&zipf](size_t /* index */)
        { return WordCountPair(zipf.next(), 1); };


    common::StatsTimerStopped run_timer, check_timer;
    size_t failures = 0, manips = 0;
    int i_outer_max = reps/loop_fct;
    for (int i_outer = 0; i_outer < i_outer_max; ++i_outer) {
        api::Run([&](Context &ctx) {
            ctx.enable_consume();
            my_rank = ctx.net.my_rank();

            const size_t num_words = words_per_worker * ctx.num_workers();

            if (i_outer == 0)
                sRLOG << "Running WordCount tests with" << manip_name
                      << "manipulator," << config_name << "config," << reps
                      << "=" << i_outer_max << "x" << loop_fct << "reps";

            for (int i_inner = -1*warmup_its; i_inner < loop_fct; ++i_inner) {

                auto driver = std::make_shared<Driver>();
                driver->silence();

                // Synchronize with barrier
                ctx.net.Barrier();
                auto traffic_before = ctx.net_manager().Traffic();
                common::StatsTimerStart current_run;

                Generate(ctx, num_words, generator)
                    .ReduceByKey(
                        common::TupleGet<0, WordCountPair>(),
                        common::TupleReduceIndex<1, WordCountPair, ReduceFn>(),
                        api::DefaultReduceConfig(), driver)
                    .Size();


                // Re-synchronize, then run final checking pass
                ctx.net.Barrier();
                current_run.Stop();
                auto traffic_precheck = ctx.net_manager().Traffic();

                common::StatsTimerStart current_check;
                auto success = driver->check(ctx);
                // No need for a barrier, it returns as soon as the global result is
                // determined
                current_check.Stop();

                if (my_rank == 0 && i_inner >= 0) { // ignore warmup
                    if (!success.first) { failures++; }
                    if (success.second) { manips++; }

                    // add current iteration timers to total
                    run_timer += current_run;
                    check_timer += current_check;
                }

                if (my_rank == 0 && i_inner >= 0) { // ignore warmup
                    auto traffic_after = ctx.net_manager().Traffic();
                    auto traffic_reduce = traffic_precheck - traffic_before;
                    auto traffic_check = traffic_after - traffic_precheck;
                    LOG1 << "RESULT"
                         << " benchmark=wordcount"
                         << " config=" << config_name
                         << " c_its=" << Config::num_parallel
                         << " c_buckets=" << Config::num_buckets
                         << " c_mod_min=" << Config::mod_min
                         << " c_mod_max=" << Config::mod_max
                         << " manip=" << manip_name
                         << " run_time=" << current_run.Microseconds()
                         << " check_time=" << current_check.Microseconds()
                         << " detection=" << success.first
                         << " manipulated=" << success.second
                         << " traffic_reduce=" << traffic_reduce.first + traffic_reduce.second
                         << " traffic_check=" << traffic_check.first + traffic_check.second
                         << " words_per_worker=" << words_per_worker
                         << " distinct_words=" << distinct_words
                         << " machines=" << ctx.num_hosts()
                         << " workers_per_host=" << ctx.workers_per_host();
                }
            }
            if (i_outer == i_outer_max - 1) { // print summary at the end
                double expected_failures = config.exp_delta * manips;
                RLOG << "WordCount with " << manip_name << " manip and "
                     << config_name << " config: "
                     << (failures > 0 ? common::log::fg_red() : "")
                     << failures << " / " << reps << " tests failed, expected approx. "
                     << expected_failures << " given " << manips << " manipulations"
                     << common::log::reset();
                sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps) << "ms;"
                      << "Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms;"
                      << "Config:" << config_name;
                sRLOG << "";
            }
        });

    }
};



auto word_count_unchecked = [](const size_t words_per_worker,
                               const size_t distinct_words,
                               const size_t seed,
                               const int reps,
                               const bool warmup = false)
{
    using Key = uint64_t;
    using Value = uint64_t;
    using WordCountPair = std::pair<Key, Value>;
    using ReduceFn = checkers::checked_plus<Value>;//std::plus<Value>;

    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();
    std::mt19937 rng(true_seed);
    zipf_generator<double> zipf(rng(), distinct_words, 1.0);
    auto generator = [&zipf](size_t /* index */)
        { return WordCountPair(zipf.next(), 1); };

    common::StatsTimerStopped run_timer;

    int i_outer_max = reps/loop_fct;
    for (int i_outer = 0; i_outer < i_outer_max; ++i_outer) {
        api::Run([&](Context &ctx) {
            ctx.enable_consume();
            my_rank = ctx.net.my_rank();

            const size_t num_words = words_per_worker * ctx.num_workers();

            if (i_outer == 0)
                sRLOG << "Running WordCount tests without checker," << reps
                      << "=" << i_outer_max << "x" << loop_fct << "reps";

            for (int i_inner = -1*warmup_its; i_inner < loop_fct; ++i_inner) {
                // Synchronize with barrier
                ctx.net.Barrier();
                auto traffic_before = ctx.net_manager().Traffic();
                common::StatsTimerStart current_run;

                Generate(ctx, num_words, generator)
                    .ReduceByKey(
                        common::TupleGet<0, WordCountPair>(),
                        common::TupleReduceIndex<1, WordCountPair, ReduceFn>())
                    .Size();


                // Re-synchronize
                ctx.net.Barrier();
                current_run.Stop();
                // add current iteration timer to total
                if (my_rank == 0 && i_inner >= 0) {
                    // ignore warmup iterations, but preserve stats for warmup runs
                    run_timer += current_run;
                }

                if (my_rank == 0 && !warmup && i_inner >= 0) { // ignore warmup
                    auto traffic_after = ctx.net_manager().Traffic();
                    auto traffic_reduce = traffic_after - traffic_before;
                    LOG1 << "RESULT"
                         << " benchmark=wordcount_unchecked"
                         << " run_time=" << current_run.Microseconds()
                         << " traffic_reduce=" << traffic_reduce.first + traffic_reduce.second
                         << " words_per_worker=" << words_per_worker
                         << " distinct_words=" << distinct_words
                         << " machines=" << ctx.num_hosts()
                         << " workers_per_host=" << ctx.workers_per_host();
                }
            }
            if (i_outer == i_outer_max - 1) { // print summary at the end
                sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
                      << "ms (no checking, no manipulation)";
                sRLOG << "";
            }
        });
    }
};

using T = uint64_t;

template <size_t num_buckets, size_t num_parallel,
          size_t log_mod_range = (8 * sizeof(size_t) - 2)>
using CRC32Config = checkers::MinireductionConfig<common::HashCrc32<T>,
                                                  num_buckets, num_parallel,
                                                  (1ULL << log_mod_range)>;

template <size_t num_buckets, size_t num_parallel,
          size_t log_mod_range = (8 * sizeof(size_t) - 2)>
using TabConfig = checkers::MinireductionConfig<common::HashTabulated<T>,
                                                num_buckets, num_parallel,
                                                (1ULL << log_mod_range)>;
template <size_t num_buckets, size_t num_parallel,
          size_t log_mod_range = (8 * sizeof(size_t) - 2)>
using Tab64Config = checkers::MinireductionConfig<
    common::TabulationHashing<sizeof(T), uint64_t>,
    num_buckets, num_parallel, (1ULL << log_mod_range)>;


namespace std {
template <typename T, typename U>
ostream& operator << (ostream& os, const pair<T, U>& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}

} // namespace std

#endif // !THRILL_EXAMPLES_CHECKERS_WORD_COUNT_HEADER
/******************************************************************************/
