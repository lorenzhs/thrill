/*******************************************************************************
 * examples/checkers/reduce_checker.hpp
 *
 * Not a real header, just factored out commons of reduce_checker{,_timings}.cpp
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
#ifndef THRILL_EXAMPLES_CHECKERS_REDUCE_CHECKER_HEADER
#define THRILL_EXAMPLES_CHECKERS_REDUCE_CHECKER_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/reduce.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <random>

using namespace thrill; // NOLINT

thread_local int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

// to subtract traffic RX/TX pairs
template <typename T, typename U>
std::pair<T, U> operator - (const std::pair<T, U>& a, const std::pair<T, U>& b) {
    return std::make_pair(a.first - b.first, a.second - b.second);
}

auto reduce_by_key = [](
    Context& ctx,
    const auto& manipulator, const auto& config,
    const std::string& manip_name,
    const std::string& config_name,
    size_t elems_per_worker, size_t seed, int reps)
{
    using Value = uint64_t;
    // checked_plus is important for making modulo efficient
    using ReduceFn = checkers::checked_plus<Value>;

    using Config = std::decay_t<decltype(config)>;
    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn, Config>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    const size_t size = elems_per_worker * ctx.num_workers();
    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();
    std::mt19937 rng(true_seed);
    std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);
    auto key_extractor = [](const Value& in) { return in & 0xFFFF; };
    auto generator = [&distribution, &rng](const size_t&) -> Value
        { return distribution(rng); };

    if (my_rank < 0) { my_rank = ctx.net.my_rank(); }
    sRLOG << "Running ReduceByKey tests with" << manip_name
          << "manipulator," << config_name << "config," << reps << "reps";

    common::StatsTimerStopped run_timer, check_timer;
    size_t failures = 0, manips = 0;
    for (int i = -3; i < reps; ++i) {
        auto driver = std::make_shared<Driver>();
        driver->silence();

        // Synchronize with barrier
        ctx.net.Barrier();
        auto traffic_before = ctx.net_manager().Traffic();
        common::StatsTimerStart current_run;

        Generate(ctx, size, generator)
            .ReduceByKey(
                VolatileKeyTag, NoDuplicateDetectionTag, key_extractor, ReduceFn(),
                api::DefaultReduceConfig(), std::hash<Value>(),
                std::equal_to<Value>(), driver)
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

        if (i >= 0) {  // ignore warmup
            // add current iteration timers to total
            run_timer += current_run;
            check_timer += current_check;

            if (!success.first) { failures++; }
            if (success.second) { manips++; }
        }

        if (my_rank == 0 && i >= 0) { // ignore warmup
            auto traffic_after = ctx.net_manager().Traffic();
            auto traffic_reduce = traffic_precheck - traffic_before;
            auto traffic_check = traffic_after - traffic_precheck;
            LOG1 << "RESULT"
                 << " benchmark=random_checked"
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
                 << " traffic_reduce=" << traffic_reduce.rx + traffic_reduce.tx
                 << " traffic_check=" << traffic_check.rx + traffic_check.tx
                 << " elems_per_worker=" << elems_per_worker
                 << " machines=" << ctx.num_hosts()
                 << " workers_per_host=" << ctx.workers_per_host();
        }
    }

    double expected_failures = config.exp_delta * manips;
    RLOG << "ReduceByKey with " << manip_name << " manip and "
         << config_name << " config: "
         << (failures > 0 ? common::log::fg_red() : "")
         << failures << " / " << reps << " tests failed, expected approx. "
         << expected_failures << " given " << manips << " manipulations"
         << common::log::reset();
    sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps) << "ms;"
          << "Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms;"
          << "Config:" << config_name;
    sRLOG << "";
};


auto reduce_by_key_unchecked = [](Context &ctx,
                                  size_t elems_per_worker, size_t seed,
                                  int reps, const bool warmup = false) {
    using Value = size_t;
    using ReduceFn = checkers::checked_plus<Value>;//std::plus<Value>;

    const size_t size = elems_per_worker * ctx.num_workers();
    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();
    std::mt19937 rng(true_seed);
    std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);
    auto key_extractor = [](const Value& in) { return in & 0xFFFF; };
    auto generator = [&distribution, &rng](const size_t&) -> Value
        { return distribution(rng); };

    if (my_rank < 0) { my_rank = ctx.net.my_rank(); }
    sRLOG << "Running ReduceByKey tests without checker," << reps << "reps";

    common::StatsTimerStopped run_timer;
    for (int i = -3; i < reps; ++i) {
        // Synchronize with barrier
        ctx.net.Barrier();
        auto traffic_before = ctx.net_manager().Traffic();
        common::StatsTimerStart current_run;

        Generate(ctx, size, generator)
            .ReduceByKey(VolatileKeyTag, key_extractor, ReduceFn())
            .Size();

        // Re-synchronize
        ctx.net.Barrier();
        current_run.Stop();
        // add current iteration timer to total
        if (i >= 0)
            run_timer += current_run;

        if (my_rank == 0 && !warmup && i >= 0) { // ignore warmup
            auto traffic_after = ctx.net_manager().Traffic();
            auto traffic_reduce = traffic_after - traffic_before;
            LOG1 << "RESULT"
                 << " benchmark=random_unchecked"
                 << " run_time=" << current_run.Microseconds()
                 << " traffic_reduce=" << traffic_reduce.rx + traffic_reduce.tx
                 << " elems_per_worker=" << elems_per_worker
                 << " machines=" << ctx.num_hosts()
                 << " workers_per_host=" << ctx.workers_per_host();
        }
    }
    sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
          << "ms (no checking, no manipulation)";
    sRLOG << "";
};

using T = size_t;

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

#endif // !THRILL_EXAMPLES_CHECKERS_REDUCE_CHECKER_HEADER

/******************************************************************************/
