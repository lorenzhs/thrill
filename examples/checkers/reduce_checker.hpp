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

auto reduce_by_key_test_factory = [](
    const auto& manipulator, const auto& config,
    const std::string& manip_name,
    const std::string& config_name,
    size_t reps)
{
    using Value = uint64_t;
    // checked_plus is important for making modulo efficient
    using ReduceFn = checkers::checked_plus<Value>;

    using Config = std::decay_t<decltype(config)>;
    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn, Config>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, manip_name, config_name](Context& ctx) {
        std::mt19937 rng(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);
        auto key_extractor = [](const Value& in) { return in & 0xFFFF; };
        auto generator = [&distribution, &rng](const size_t&) -> Value
            { return distribution(rng); };

        ctx.enable_consume();
        if (my_rank < 0) { my_rank = ctx.net.my_rank(); }
        sRLOG << "Running ReduceByKey tests with" << manip_name
              << "manipulator," << config_name << "config," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();

            // Synchronize with barrier
            ctx.net.Barrier();
            auto traffic_before = ctx.net_manager().Traffic();

            common::StatsTimerStart current_run;
            Generate(ctx, 1000000, generator)
                .ReduceByKey(
                    VolatileKeyTag, NoDuplicateDetectionTag, key_extractor, ReduceFn(),
                    api::DefaultReduceConfig(), std::hash<Value>(),
                    std::equal_to<Value>(), driver)
                .Size();
            current_run.Stop();

            // Re-synchronize, then run final checking pass
            ctx.net.Barrier();
            auto traffic_precheck = ctx.net_manager().Traffic();

            common::StatsTimerStart current_check;
            auto success = driver->check(ctx);
            current_check.Stop();

            if (!success.first) { failures++; }
            if (success.second) { manips++; }

            ctx.net.Barrier();

            // add current iteration timers to total
            run_timer += current_run;
            check_timer += current_check;

            if (ctx.my_rank() == 0) {
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
                     << " traffic_reduce=" << traffic_reduce.first + traffic_reduce.second
                     << " traffic_check=" << traffic_check.first + traffic_check.second
                     << " machines=" << ctx.num_hosts()
                     << " workers_per_host=" << ctx.workers_per_host();
            }
        }

        RLOG << "ReduceByKey with " << manip_name << " manip and "
             << config_name << " config: "
             << (failures > 0 ? common::log::fg_red() : "")
             << failures << " / " << reps << " tests failed"
             << "; " << manips << " manipulations" << common::log::reset();
        sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps) << "ms;"
              << "Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms;"
              << "Config:" << config_name;
        sRLOG << "";
    };
};


auto reduce_by_key_unchecked = [](size_t reps) {
    using Value = size_t;
    using ReduceFn = checkers::checked_plus<Value>;//std::plus<Value>;

    return [reps](Context& ctx) {
        std::mt19937 rng(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);
        auto key_extractor = [](const Value& in) { return in & 0xFFFF; };
        auto generator = [&distribution, &rng](const size_t&) -> Value
            { return distribution(rng); };

        ctx.enable_consume();
        if (my_rank < 0) { my_rank = ctx.net.my_rank(); }
        sRLOG << "Running ReduceByKey tests without checker," << reps << "reps";

        common::StatsTimerStopped run_timer;
        for (size_t i = 0; i < reps; ++i) {
            // Synchronize with barrier
            ctx.net.Barrier();
            auto traffic_before = ctx.net_manager().Traffic();
            common::StatsTimerStart current_run;
            Generate(ctx, 1000000, generator)
                .ReduceByKey(VolatileKeyTag, key_extractor, ReduceFn())
                .Size();
            current_run.Stop();

            // Re-synchronize
            ctx.net.Barrier();
            // add current iteration timer to total
            run_timer += current_run;

            if (ctx.my_rank() == 0) {
                auto traffic_after = ctx.net_manager().Traffic();
                auto traffic_reduce = traffic_after - traffic_before;
                LOG1 << "RESULT"
                     << " benchmark=random_unchecked"
                     << " run_time=" << current_run.Microseconds()
                     << " traffic_reduce=" << traffic_reduce.first + traffic_reduce.second
                     << " machines=" << ctx.num_hosts()
                     << " workers_per_host=" << ctx.workers_per_host();
            }
        }
        sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
              << "ms (no checking, no manipulation)";
        sRLOG << "";
    };
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

namespace std {
template <typename T, typename U>
ostream& operator << (ostream& os, const pair<T, U>& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}

} // namespace std

#endif // !THRILL_EXAMPLES_CHECKERS_REDUCE_CHECKER_HEADER

/******************************************************************************/
