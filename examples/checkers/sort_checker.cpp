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
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

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
const size_t default_size = 1000000;
const size_t default_distinct = 100000000;

thread_local static int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

using T = int;

// to subtract traffic RX/TX pairs
template <typename T, typename U>
std::pair<T, U> operator - (const std::pair<T, U>& a, const std::pair<T, U>& b) {
    return std::make_pair(a.first - b.first, a.second - b.second);
}


template <typename Manipulator, typename HashFn>
void sort_random(const Manipulator& /*manipulator*/, const HashFn& /*hash*/,
                 const std::string& manip_name,
                 const std::string& config_name,
                 size_t size, size_t distinct, int reps) {
    using Value = T;
    using Compare = std::less<Value>;
    using Checker = checkers::SortChecker<Value, Compare, HashFn>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    api::Run([&](Context& ctx) {
        ctx.enable_consume();
        my_rank = ctx.net.my_rank();

        std::default_random_engine gen(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, distinct);
        auto generator = [&distribution, &gen](const size_t&) -> Value
            { return distribution(gen); };

        sRLOG << "Running sort tests with" << manip_name << "manip and"
              << config_name << "config," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, dummy = 0, manips = 0;
        for (int i = -3; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();

            // Synchronize with barrier
            ctx.net.Barrier();
            auto traffic_before = ctx.net_manager().Traffic();
            common::StatsTimerStart current_run;

            size_t force_eval =
                Generate(ctx, size, generator)
                .Sort(Compare{}, driver)
                .Size();
            dummy += force_eval;

            // Re-synchronize, then run final checking pass
            ctx.net.Barrier();
            current_run.Stop();
            auto traffic_precheck = ctx.net_manager().Traffic();

            common::StatsTimerStart current_check;
            auto success = driver->check(ctx);
            current_check.Stop();

            if (i >= 0) { // ignore warmup
                if (!success.first) { failures++; }
                if (success.second) { manips++; }

                // add current iteration timers to total
                run_timer += current_run;
                check_timer += current_check;
            }

            if (my_rank == 0 && i >= 0) { // ignore warmup
                auto traffic_after = ctx.net_manager().Traffic();
                auto traffic_sort = traffic_precheck - traffic_before;
                auto traffic_check = traffic_after - traffic_precheck;
                LOG1 << "RESULT benchmark=sort"
                     << " config=" << config_name
                     << " manip=" << manip_name
                     << " size=" << size
                     << " distinct=" << distinct
                     << " run_time=" << current_run.Microseconds()
                     << " check_time=" << current_check.Microseconds()
                     << " detection=" << success.first
                     << " manipulated=" << success.second
                     << " traffic_sort=" << traffic_sort.first + traffic_sort.second
                     << " traffic_check=" << traffic_check.first + traffic_check.second
                     << " machines=" << ctx.num_hosts()
                     << " workers_per_host=" << ctx.workers_per_host();
            }
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
    });
}


void sort_unchecked(size_t size, size_t distinct, int reps = 100,
                    bool warmup = false) {
    using Value = T;
    using Compare = std::less<Value>;

    api::Run([&](Context& ctx) {
        ctx.enable_consume();
        my_rank = ctx.net.my_rank();

        std::default_random_engine gen(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, distinct);
        auto generator = [&distribution, &gen](const size_t&) -> Value
            { return distribution(gen); };

        sRLOG << "Running sort tests without checker," << reps << "reps";

        common::StatsTimerStopped run_timer;
        size_t dummy = 0;
        for (int i = -3; i < reps; ++i) {
            // Synchronize with barrier
            ctx.net.Barrier();
            auto traffic_before = ctx.net_manager().Traffic();
            common::StatsTimerStart current_run;

            size_t force_eval =
                Generate(ctx, size, generator)
                .Sort(Compare{})
                .Size();
            dummy += force_eval;

            // Re-synchronize
            ctx.net.Barrier();
            current_run.Stop();

            if (i >= 0) run_timer += current_run;

            if (my_rank == 0 && !warmup && i >= 0) { // ignore warmup
                auto traffic_after = ctx.net_manager().Traffic();
                auto traffic_sort = traffic_after - traffic_before;
                LOG1 << "RESULT benchmark=sort_unchecked"
                     << " size=" << size
                     << " distinct=" << distinct
                     << " run_time=" << current_run.Microseconds()
                     << " traffic_sort=" << traffic_sort.first + traffic_sort.second
                     << " machines=" << ctx.num_hosts()
                     << " workers_per_host=" << ctx.workers_per_host();
            }

        }

        sRLOG << "Sort:" << run_timer.Microseconds()/(1000.0*reps)
              << "ms (no checking, no manipulation)\n";
    });
}

template <size_t bits>
using CRC32Config = common::masked_hash<T, bits, common::HashCrc32<T>>;

template <size_t bits>
using TabConfig = common::masked_hash<T, bits, common::HashTabulated<T>>;

static const std::vector<std::string> known_configs = {
    "CRC32", "Tab", "CRC32-16", "Tab-16", "CRC32-8", "Tab-8",
    "CRC32-4", "Tab-4", "CRC32-2", "Tab-2"
};

template <typename Functor, typename Manipulator>
void run(Functor &&test, const Manipulator &manip, const std::string& name) {
#ifdef CHECKERS_FULL
    test(common::HashCrc32<T>{}, "CRC32", manip, name);
    test(common::HashTabulated<T>{}, "Tab", manip, name);

    test(CRC32Config<16>{ }, "CRC32-16", manip, name);
    test(TabConfig<16>{ }, "Tab-16", manip, name);

    test(CRC32Config<8>{ }, "CRC32-8", manip, name);
    test(TabConfig<8>{ }, "Tab-8", manip, name);
#endif
    test(CRC32Config<4>{ }, "CRC32-4", manip, name);
    test(TabConfig<4>{ }, "Tab-4", manip, name);
    test(CRC32Config<2>{ }, "CRC32-2", manip, name);
    test(TabConfig<2>{ }, "Tab-2", manip, name);
}

// yikes, preprocessor
#define TEST_CHECK(MANIP) if (run_ ## MANIP) \
    run(test, checkers::SortManipulator ## MANIP(), #MANIP)

// run with template parameter
#define TEST_CHECK_T(NAME, FULL) \
    run(test, checkers::SortManipulator ## FULL(), #NAME)

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    int reps = default_reps;
    size_t size = default_size, distinct = default_distinct, seed = 42;
    std::string config_param = "Tab-2";
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('s', "size", size, "input size");
    clp.add_size_t('d', "distinct", distinct, "number of distinct elements");
    clp.add_size_t('e', "seed", seed, "seed for input generation (0: random)");
    clp.add_string('c', "config", config_param, "which configuration to run");

    bool run_unchecked = false, run_Dummy = false, run_Bitflip = false,
        run_IncFirst = false, run_RandFirst = false, run_ResetToDefault = false,
        run_SetEqual = false;
    clp.add_flag('u', "unchecked", run_unchecked, "run unchecked");
    clp.add_flag('x', "Dummy", run_Dummy, "run Dummy manip");
    clp.add_flag('i', "IncFirst", run_IncFirst, "run IncFirst manip");
    clp.add_flag('b', "Bitflip", run_Bitflip, "run Bitflip manip");
    clp.add_flag('f', "RandFirst", run_RandFirst, "run RandFirst manip (boring)");
    clp.add_flag('r', "ResetToDefault", run_ResetToDefault, "run ResetToDefault manip");
    clp.add_flag('e', "SetEqual", run_SetEqual, "run SetEqual manip");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    if (std::find(known_configs.begin(), known_configs.end(), config_param) ==
        known_configs.end()) {
        LOG1 << "unknown config: " << config_param;
        return 1;
    }

    auto test = [&config_param, size, distinct, reps]
        (auto config, const std::string& config_name,
         auto &manipulator, const std::string& manip_name) {
        if (config_name != config_param) {
            return;
        }
        sort_random(manipulator, config, manip_name, config_name,
                    size, distinct, reps);
    };

    // Warmup
    sort_unchecked(size, distinct, std::min(100, reps), true);

    if (run_unchecked) sort_unchecked(size, distinct, reps);
    TEST_CHECK(Dummy);
    TEST_CHECK(IncFirst);
    TEST_CHECK(Bitflip);
    TEST_CHECK(RandFirst);  // random value is easily caught
    // TEST_CHECK(DropLast);  // disabled: always caught by size check
    TEST_CHECK(ResetToDefault);
    // TEST_CHECK(AddToEmpty);  // disabled: always caught by size check
    TEST_CHECK(SetEqual);
    // TEST_CHECK(DuplicateLast);  // disabled: always caught by size check
    // TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>); // disabled: boring
}

/******************************************************************************/
