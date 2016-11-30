/*******************************************************************************
 * examples/checkers/sort_checker.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/sort.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

using namespace thrill; // NOLINT

#ifdef CHECKERS_FULL
const size_t default_reps = 100000;
#else
const size_t default_reps = 100;
#endif

thread_local static int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

auto sort_random = [](const auto &manipulator, const auto &hash,
                      const std::string& manip_name,
                      const std::string& config_name,
                      size_t reps) {
    using Value = int;
    using Compare = std::less<Value>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using HashFn = std::decay_t<decltype(hash)>;
    using Checker = checkers::SortChecker<Value, Compare, HashFn>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, manip_name, config_name](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) { my_rank = ctx.net.my_rank(); }

        sRLOG << "Running sort tests with" << manip_name << "manip and"
              << config_name << "config," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();

            // Synchronize with barrier
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .Sort(Compare{}, driver)
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

        RLOG << "Sort with " << manip_name << " manip and "
             << config_name << " config: "
             << (failures > 0 ? common::log::fg_red() : "")
             << failures << " / " << reps << " tests failed; "
             << manips << " manipulations" << common::log::reset();

        sRLOG
            << "Sort:" << run_timer.Microseconds()/(1000.0*reps) << "ms;"
            << "Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms;"
            << "Config:" << config_name << "\n";
    };
};


auto sort_unchecked = [](size_t reps = 100) {
    using Value = int;
    using Compare = std::less<Value>;

    return [reps](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) { my_rank = ctx.net.my_rank(); }

        sRLOG << "Running sort tests without checker," << reps << "reps";

        common::StatsTimerStopped run_timer;
        size_t dummy = 0;
        for (size_t i = 0; i < reps; ++i) {
            // Synchronize with barrier
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .Sort(Compare{})
                .Size();
            run_timer.Stop();
            dummy += force_eval;
        }

        sRLOG << "Sort:" << run_timer.Microseconds()/(1000.0*reps)
              << "ms (no checking, no manipulation)\n";
    };
};

template <size_t bits>
using Hash = common::masked_hash<int, bits>;

auto run = [](const auto &manipulator, const std::string &name, size_t reps) {

#ifdef CHECKERS_FULL
    //api::Run(sort_random(manipulator, Hash<32>{}, name, "CRC32-32", reps));
    api::Run(sort_random(manipulator, Hash<16>{}, name, "CRC32-16", reps));
#endif
    api::Run(sort_random(manipulator, Hash<8>{}, name, "CRC32-8", reps));
    api::Run(sort_random(manipulator, Hash<4>{}, name, "CRC32-4", reps));
    api::Run(sort_random(manipulator, Hash<2>{}, name, "CRC32-2", reps));
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) \
    run(checkers::SortManipulator ## MANIP(), #MANIP, reps)
#define TEST_CHECK_A(MANIP, ...) \
    run(checkers::SortManipulator ## MANIP(), #MANIP, __VA_ARGS__)

// run with template parameter
#define TEST_CHECK_T(NAME, FULL) \
    run(checkers::SortManipulator ## FULL(), #NAME, reps)

int main(int argc, char **argv) {
    thrill::common::CmdlineParser clp;

    size_t reps = default_reps;
    clp.AddSizeT('n', "iterations", reps, "iterations");

    if (!clp.Process(argc, argv)) return -1;
    clp.PrintResult();

    //api::Run(sort_unchecked(reps));
    //TEST_CHECK_A(Dummy, std::min(reps, (size_t)100));
    TEST_CHECK(IncFirst);
    // TEST_CHECK(RandFirst);  // disabled: random value is easily caught
    // TEST_CHECK(DropLast);  // disabled: always caught by size check
    TEST_CHECK(ResetToDefault);
    // TEST_CHECK(AddToEmpty);  // disabled: always caught by size check
    TEST_CHECK(SetEqual);
    // TEST_CHECK(DuplicateLast);  // disabled: always caught by size check
    // TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>); // disabled: boring
}

/******************************************************************************/
