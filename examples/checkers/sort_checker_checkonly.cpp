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
#include <thrill/common/aggregate.hpp>
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
constexpr int warmup_its = 10;

thread_local static int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

using T = int;

// to subtract traffic RX/TX pairs
template <typename T, typename U>
std::pair<T, U> operator - (const std::pair<T, U>& a, const std::pair<T, U>& b) {
    return std::make_pair(a.first - b.first, a.second - b.second);
}


template <typename HashFn>
void sort_checkonly(const HashFn& /*hash*/, const std::string& config_name,
                    size_t size, size_t distinct, size_t seed, int reps) {
    using Value = T;
    using Compare = std::less<Value>;
    using Checker = checkers::SortChecker<Value, Compare, HashFn>;

    size_t true_seed = seed;
    if (seed == 0) true_seed = std::random_device{}();

    common::Aggregate<double> generate_time, check_time;
    int i_outer_max = (reps - 1)/loop_fct + 1;
    for (int i_outer = 0; i_outer < i_outer_max; ++i_outer) {
        api::Run([&](Context& ctx) {
            ctx.enable_consume();
            my_rank = ctx.net.my_rank();
            const size_t local_size = size / ctx.num_workers();

            std::default_random_engine gen(true_seed + my_rank);
            std::uniform_int_distribution<Value> distribution(0, distinct);
            auto generator = [&distribution, &gen](const size_t&) -> Value
                { return distribution(gen); };

            // advance seed for next round
            ctx.net.Barrier();
            if (my_rank == 0) true_seed += ctx.num_workers();

            sRLOG << "Running sort checkonly tests with" << config_name
                  << "config," << reps << "reps";

            for (int i_inner = -1*warmup_its; i_inner < loop_fct && i_inner < reps; ++i_inner) {
                // Synchronize with barrier
                ctx.net.Barrier();

                common::StatsTimerStart t_generate;
                std::vector<Value> input;
                input.reserve(local_size);
                for (size_t i = 0; i < local_size; ++i) {
                    input.emplace_back(generator(i));
                }
                ctx.net.Barrier();
                t_generate.Stop();

                common::StatsTimerStart t_check;
                Checker checker;
                checker.reset(); // checker needs to be reset to initialize
                for (const auto &elem : input) {
                    checker.add_pre(elem);
                }
                ctx.net.Barrier();
                t_check.Stop();

                if (my_rank == 0 && i_inner >= 0) { // ignore warmup
                    // add current iteration timers to total
                    generate_time.Add(t_generate.Microseconds() / 1000.0);
                    check_time.Add(t_check.Microseconds() / 1000.0);

                    LOG1 << "RESULT benchmark=sort"
                         << " config=" << config_name
                         << " size=" << size
                         << " distinct=" << distinct
                         << " gen_time=" << t_generate.Microseconds()
                         << " check_time=" << t_check.Microseconds()
                         << " hashbits=" << Checker::HashBits
                         << " machines=" << ctx.num_hosts()
                         << " workers_per_host=" << ctx.workers_per_host();
                }
            }

            if (i_outer == i_outer_max - 1) { // print summary at the end
                RLOG << "Sort checkonly, Check: " << check_time.Mean()
                     << "ms (" << check_time.StDev() << "); Generate: "
                     << generate_time.Mean() << "ms (" << generate_time.StDev()
                     << "); Config: " << config_name << " - CHECKONLY MODE";
                RLOG << "";
            }
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

template <typename Functor>
void run(Functor &&test) {
#ifdef CHECKERS_FULL
    test(common::HashCrc32<T>{}, "CRC32");
    test(common::HashTabulated<T>{}, "Tab");
#endif

    test(CRC32Config<16>{ }, "CRC32-16");
    test(TabConfig<16>{ }, "Tab-16");

    test(CRC32Config<12>{ }, "CRC32-12");
    test(TabConfig<12>{ }, "Tab-12");

    test(CRC32Config<8>{ }, "CRC32-8");
    test(TabConfig<8>{ }, "Tab-8");

    test(CRC32Config<6>{ }, "CRC32-6");
    test(TabConfig<6>{ }, "Tab-6");

    test(CRC32Config<4>{ }, "CRC32-4");
    test(TabConfig<4>{ }, "Tab-4");

    test(CRC32Config<3>{ }, "CRC32-3");
    test(TabConfig<3>{ }, "Tab-3");

    test(CRC32Config<2>{ }, "CRC32-2");
    test(TabConfig<2>{ }, "Tab-2");

    test(CRC32Config<1>{ }, "CRC32-1");
    test(TabConfig<1>{ }, "Tab-1");
}

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

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    if (std::find(known_configs.begin(), known_configs.end(), config_param) ==
        known_configs.end()) {
        LOG1 << "unknown config: " << config_param;
        return 1;
    }

    auto test = [&config_param, size, distinct, seed, reps]
        (auto config, const std::string& config_name) {
        if (config_name != config_param) {
            return;
        }
        sort_checkonly(config, config_name, size, distinct, seed, reps);
    };

    run(test);
}

/******************************************************************************/
