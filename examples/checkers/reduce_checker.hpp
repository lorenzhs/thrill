#pragma once

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

auto reduce_by_key_test_factory = [](
    const auto& manipulator, const auto& config,
    const std::string& manip_name,
    const std::string& config_name,
    size_t reps)
{
    using Value = uint64_t;
    using ReduceFn = checkers::checked_plus<Value>;
    //using ReduceFn = std::plus<Value>;

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
            run_timer.Start();
            Generate(ctx, 1000000, generator)
                .ReduceByKey(
                    VolatileKeyTag, NoDuplicateDetectionTag, key_extractor, ReduceFn(),
                    api::DefaultReduceConfig(), std::hash<Value>(),
                    std::equal_to<Value>(), driver)
                .Size();
            run_timer.Stop();

            // Re-synchronize, then run final checking pass
            ctx.net.Barrier();
            check_timer.Start();
            auto success = driver->check(ctx);
            check_timer.Stop();

            if (!success.first) { failures++; }
            if (success.second) { manips++; }
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
            ctx.net.Barrier();
            run_timer.Start();
            Generate(ctx, 1000000, generator)
                .ReduceByKey(VolatileKeyTag, key_extractor, ReduceFn())
                .Execute();
            run_timer.Stop();
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

auto run = [](const auto &manipulator, const std::string& name, size_t reps) {

    auto& f = reduce_by_key_test_factory;

#ifndef CHECKERS_FULL
    // default
    api::Run(f(manipulator, CRC32Config<16, 1, 31>{}, name, "1x16 m31 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 1, 15>{}, name, "1x16 m15 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 1, 7>{}, name, "1x16 m7 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 1, 3>{}, name, "1x16 m3 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 1>{}, name, "1x16 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 1>{}, name, "1x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 1>{}, name, "1x2 CRC32", reps));
#else
    api::Run(f(manipulator, CRC32Config<256, 2>{}, name, "2x256 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<256, 1>{}, name, "1x256 CRC32", reps));

    api::Run(f(manipulator, CRC32Config<16, 4>{}, name, "4x16 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 2>{}, name, "2x16 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<16, 1>{}, name, "1x16 CRC32", reps));

    api::Run(f(manipulator, CRC32Config<4, 8>{}, name, "8x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 6>{}, name, "6x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 4>{}, name, "4x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 3>{}, name, "3x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 2>{}, name, "2x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 1>{}, name, "1x4 CRC32", reps));

    api::Run(f(manipulator, CRC32Config<2, 8>{}, name, "8x2 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 6>{}, name, "6x2 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 4>{}, name, "4x2 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 3>{}, name, "3x2 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 2>{}, name, "2x2 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<2, 1>{}, name, "1x2 CRC32", reps));
#endif

#ifndef CHECKERS_FULL
    /*
    api::Run(f(manipulator, TabConfig<16, 1, 31>{}, name, "1x16 m31 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 1, 15>{}, name, "1x16 m15 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 1, 7>{}, name, "1x16 m7 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 1, 3>{}, name, "1x16 m3 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 1>{}, name, "1x16 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 1>{}, name, "1x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 1>{}, name, "1x2 Tab", reps));
    */
#else
    api::Run(f(manipulator, TabConfig<256, 2>{}, name, "2x256 Tab", reps));
    api::Run(f(manipulator, TabConfig<256, 1>{}, name, "1x256 Tab", reps));

    api::Run(f(manipulator, TabConfig<16, 4>{}, name, "4x16 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 2>{}, name, "2x16 Tab", reps));
    api::Run(f(manipulator, TabConfig<16, 1>{}, name, "1x16 Tab", reps));

    api::Run(f(manipulator, TabConfig<4, 8>{}, name, "8x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 6>{}, name, "6x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 4>{}, name, "4x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 3>{}, name, "3x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 2>{}, name, "2x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 1>{}, name, "1x4 Tab", reps));

    api::Run(f(manipulator, TabConfig<2, 8>{}, name, "8x2 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 6>{}, name, "6x2 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 4>{}, name, "4x2 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 3>{}, name, "3x2 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 2>{}, name, "2x2 Tab", reps));
    api::Run(f(manipulator, TabConfig<2, 1>{}, name, "1x2 Tab", reps));
#endif
};

namespace std {
template <typename T, typename U>
ostream& operator << (ostream& os, const pair<T, U>& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}

} // namespace std
