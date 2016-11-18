/*******************************************************************************
 * examples/checkers/reduce_checker.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

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

#ifdef CHECKERS_FULL
const size_t default_reps = 10000;
#else
const size_t default_reps = 100;
#endif

thread_local int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

auto reduce_by_key_test_factory = [](
    const auto &manipulator, const auto &config,
    const std::string& manip_name,
    const std::string& config_name,
    size_t reps)
{
    using Value = uint64_t;
    using ReduceFn = std::plus<Value>;

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
        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();

            // Synchronize with barrier
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(ctx, 1000000, generator)
                .ReduceByKey(
                    VolatileKeyTag, key_extractor, ReduceFn(),
                    api::DefaultReduceConfig(), driver)
                .Size();
            run_timer.Stop();
            dummy += force_eval;

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
    using ReduceFn = std::plus<Value>;

    return [reps](Context& ctx) {
        std::mt19937 rng(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);
        auto key_extractor = [](const Value& in) { return in & 0xFFFF; };
        auto generator = [&distribution, &rng](const size_t&) -> Value
            { return distribution(rng); };

        ctx.enable_consume();
        if (my_rank < 0) { my_rank = ctx.net.my_rank(); }
        sRLOG << "Running ReduceByKey tests without checker," << reps << "reps";

        size_t dummy = 0;
        common::StatsTimerStopped run_timer;
        for (size_t i = 0; i < reps; ++i) {
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(ctx, 1000000, generator)
                .ReduceByKey(VolatileKeyTag, key_extractor, ReduceFn())
                .Size();
            dummy += force_eval;
            run_timer.Stop();
        }

        sRLOG << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
              << "ms (no checking, no manipulation)";
        sRLOG << "";
    };
};

using T = size_t;

template <size_t bucket_bits, size_t num_parallel>
using CRC32Config = checkers::MinireductionConfig<common::hash_crc32<T>,
                                                  bucket_bits, num_parallel>;

template <size_t bucket_bits, size_t num_parallel>
using TabConfig = checkers::MinireductionConfig<common::hash_tabulated<T>,
                                                bucket_bits, num_parallel>;

auto run = [](const auto &manipulator, const std::string& name,
              size_t reps = default_reps) {

    auto& f = reduce_by_key_test_factory;

    api::Run(f(manipulator, CRC32Config<8, 4>{}, name, "4x8 CRC32", reps));
#ifdef CHECKERS_FULL
    api::Run(f(manipulator, CRC32Config<8, 3>{}, name, "3x8 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<8, 2>{}, name, "2x8 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<8, 1>{}, name, "1x8 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 8>{}, name, "8x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 6>{}, name, "6x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 4>{}, name, "4x4 CRC32", reps));
    api::Run(f(manipulator, CRC32Config<4, 2>{}, name, "2x4 CRC32", reps));
#endif

    api::Run(f(manipulator, TabConfig<8, 4>{}, name, "4x8 Tab", reps));
#ifdef CHECKERS_FULL
    api::Run(f(manipulator, TabConfig<8, 3>{}, name, "3x8 Tab", reps));
    api::Run(f(manipulator, TabConfig<8, 2>{}, name, "2x8 Tab", reps));
    api::Run(f(manipulator, TabConfig<8, 1>{}, name, "1x8 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 8>{}, name, "8x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 6>{}, name, "6x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 4>{}, name, "4x4 Tab", reps));
    api::Run(f(manipulator, TabConfig<4, 2>{}, name, "2x4 Tab", reps));
#endif
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) run(checkers::ReduceManipulator ## MANIP(), #MANIP)
#define TEST_CHECK_I(MANIP, ITS) run(checkers::ReduceManipulator ## MANIP(), #MANIP, ITS)

namespace std {
template <typename T, typename U>
ostream& operator << (ostream& os, const pair<T, U>& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}
} // namespace std

int main() {
    api::Run(reduce_by_key_unchecked(default_reps));
    TEST_CHECK_I(Dummy, std::min(default_reps, (size_t)100));
    TEST_CHECK(RandFirstKey);
    TEST_CHECK(SwitchValues);
    TEST_CHECK(DropFirst);
    TEST_CHECK(IncFirst);
    TEST_CHECK(RandFirst);
    TEST_CHECK(IncFirstKey);
}

/******************************************************************************/
