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

constexpr int loop_fct = 1000;
constexpr int warmup_its = 3;

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
                 size_t size, size_t distinct, size_t seed, int reps) {
    using Value = T;
    using Compare = std::less<Value>;
    using Checker = checkers::SortChecker<Value, Compare, HashFn>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();

    common::StatsTimerStopped run_timer, check_timer;
    size_t failures = 0, dummy = 0, manips = 0;
    int i_outer_max = (reps - 1)/loop_fct + 1;
    for (int i_outer = 0; i_outer < i_outer_max; ++i_outer) {
        api::Run([&](Context& ctx) {
            ctx.enable_consume();
            my_rank = ctx.net.my_rank();

            std::default_random_engine gen(true_seed + my_rank);
            std::uniform_int_distribution<Value> distribution(0, distinct);
            auto generator = [&distribution, &gen](const size_t&) -> Value
                { return distribution(gen); };

            // advance seed for next round
            ctx.net.Barrier();
            if (my_rank == 0) true_seed += ctx.num_workers();

            sRLOG << "Running sort tests with" << manip_name << "manip and"
                  << config_name << "config," << reps << "reps";

            for (int i_inner = -1*warmup_its; i_inner < loop_fct && i_inner < reps; ++i_inner) {
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

                if (my_rank == 0 && i_inner >= 0) { // ignore warmup
                    if (!success.first) { failures++; }
                    if (success.second) { manips++; }

                    // add current iteration timers to total
                    run_timer += current_run;
                    check_timer += current_check;

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
                         << " success=" << success.first
                         << " manipulated=" << success.second
                         << " traffic_sort=" << traffic_sort.first + traffic_sort.second
                         << " traffic_check=" << traffic_check.first + traffic_check.second
                         << " hashbits=" << Checker::HashBits
                         << " machines=" << ctx.num_hosts()
                         << " workers_per_host=" << ctx.workers_per_host();
                }
            }

            if (i_outer == i_outer_max - 1) { // print summary at the end
                RLOG << "Sort with " << manip_name << " manip and "
                     << config_name << " config: "
                     << (failures > 0 ? common::log::fg_red() : "")
                     << failures << " / " << reps << " tests failed; "
                     << manips << " manipulations" << common::log::reset();

                sRLOG
                    << "Sort:" << run_timer.Microseconds()/(1000.0*reps) << "ms;"
                    << "Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms;"
                    << "Config:" << config_name << "\n";
            }
        });
    }
}


void sort_unchecked(const size_t size, const size_t distinct, const size_t seed,
                    const int reps, const bool warmup = false) {
    using Value = T;
    using Compare = std::less<Value>;

    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();

    common::StatsTimerStopped run_timer;
    size_t dummy = 0;
    int i_outer_max = (reps - 1)/loop_fct + 1;
    for (int i_outer = 0; i_outer < i_outer_max; ++i_outer) {
        api::Run([&](Context& ctx) {
            ctx.enable_consume();
            my_rank = ctx.net.my_rank();

            std::default_random_engine gen(true_seed + my_rank);
            std::uniform_int_distribution<Value> distribution(0, distinct);
            auto generator = [&distribution, &gen](const size_t&) -> Value
                { return distribution(gen); };

            // advance seed for next round
            ctx.net.Barrier();
            if (my_rank == 0) true_seed += ctx.num_workers();

            sRLOG << "Running sort tests without checker," << reps << "reps";

            for (int i_inner = -1*warmup_its; i_inner < loop_fct && i_inner < reps; ++i_inner) {
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

                if (my_rank == 0 && i_inner >= 0) run_timer += current_run;

                if (my_rank == 0 && !warmup && i_inner >= 0) { // ignore warmup
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

            if (i_outer == i_outer_max - 1)
                sRLOG << "Sort:" << run_timer.Microseconds()/(1000.0*reps)
                      << "ms (no checking, no manipulation)\n";
        });
    }
}

template <size_t bits>
using CRC32Config = common::masked_hash<T, bits, common::HashCrc32<T>>;

template <size_t bits>
using TabConfig = common::masked_hash<T, bits, common::HashTabulated<T>>;

static const std::vector<std::string> known_configs = {
    "CRC32", "Tab", "CRC32-16", "Tab-16", "CRC32-12", "Tab-12",
    "CRC32-8", "Tab-8", "CRC32-6", "Tab-6", "CRC32-4", "Tab-4",
    "CRC32-3", "Tab-3", "CRC32-2", "Tab-2", "CRC32-1", "Tab-1"
};

template <typename Functor, typename Manipulator>
void run(Functor &&test, const Manipulator &manip, const std::string& name) {
#ifdef CHECKERS_FULL
    test(common::HashCrc32<T>{}, "CRC32", manip, name);
    test(common::HashTabulated<T>{}, "Tab", manip, name);
#endif

    test(CRC32Config<16>{ }, "CRC32-16", manip, name);
    test(TabConfig<16>{ }, "Tab-16", manip, name);

    test(CRC32Config<12>{ }, "CRC32-12", manip, name);
    test(TabConfig<12>{ }, "Tab-12", manip, name);

    test(CRC32Config<8>{ }, "CRC32-8", manip, name);
    test(TabConfig<8>{ }, "Tab-8", manip, name);

    test(CRC32Config<6>{ }, "CRC32-6", manip, name);
    test(TabConfig<6>{ }, "Tab-6", manip, name);

    test(CRC32Config<4>{ }, "CRC32-4", manip, name);
    test(TabConfig<4>{ }, "Tab-4", manip, name);

    test(CRC32Config<3>{ }, "CRC32-3", manip, name);
    test(TabConfig<3>{ }, "Tab-3", manip, name);

    test(CRC32Config<2>{ }, "CRC32-2", manip, name);
    test(TabConfig<2>{ }, "Tab-2", manip, name);

    test(CRC32Config<1>{ }, "CRC32-1", manip, name);
    test(TabConfig<1>{ }, "Tab-1", manip, name);
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
        run_Inc = false, run_Rand = false, run_ResetToDefault = false,
        run_SetEqual = false;
    clp.add_flag('u', "unchecked", run_unchecked, "run unchecked");
    clp.add_flag('x', "Dummy", run_Dummy, "run Dummy manip");
    clp.add_flag('i', "Inc", run_Inc, "run Inc manip");
    clp.add_flag('b', "Bitflip", run_Bitflip, "run Bitflip manip");
    clp.add_flag('f', "Rand", run_Rand, "run Rand manip (boring)");
    clp.add_flag('r', "ResetToDefault", run_ResetToDefault, "run ResetToDefault manip");
    clp.add_flag('q', "SetEqual", run_SetEqual, "run SetEqual manip");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    if (std::find(known_configs.begin(), known_configs.end(), config_param) ==
        known_configs.end()) {
        LOG1 << "unknown config: " << config_param;
        return 1;
    }

    auto test = [&config_param, size, distinct, seed, reps]
        (auto config, const std::string& config_name,
         auto &manipulator, const std::string& manip_name) {
        if (config_name != config_param) {
            return;
        }
        sort_random(manipulator, config, manip_name, config_name,
                    size, distinct, seed, reps);
    };

    // Warmup
    sort_unchecked(size, distinct, seed, std::min(100, reps), true);

    if (run_unchecked) sort_unchecked(size, distinct, seed, reps);
    TEST_CHECK(Dummy);
    TEST_CHECK(Inc);
    TEST_CHECK(Bitflip);
    TEST_CHECK(Rand);  // random value is easily caught
    // TEST_CHECK(DropLast);  // disabled: always caught by size check
    TEST_CHECK(ResetToDefault);
    // TEST_CHECK(AddToEmpty);  // disabled: always caught by size check
    TEST_CHECK(SetEqual);
    // TEST_CHECK(DuplicateLast);  // disabled: always caught by size check
    // TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>); // disabled: boring
}

/******************************************************************************/
