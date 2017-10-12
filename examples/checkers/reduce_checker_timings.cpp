/*******************************************************************************
 * examples/checkers/reduce_checker_timings.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <tlx/cmdline_parser.hpp>

#include "reduce_checker.hpp"
#include "timings.hpp"

#ifdef CHECKERS_FULL
const int default_reps = 10000;
#else
const int default_reps = 100;
#endif
const size_t default_elems_per_worker = 125000;

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    int reps = default_reps;
    size_t elems_per_worker = default_elems_per_worker, seed = 42;
    std::string config_param = "8x16_CRC32_m15";
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('e', "elems", elems_per_worker, "elements per worker");
    clp.add_size_t('s', "seed", seed, "seed for input generation (0: random)");
    clp.add_string('c', "config", config_param, "which configuration to run");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run([&](Context& ctx){
        ctx.enable_consume();
        my_rank = ctx.net.my_rank();
        // warmup
        RLOG << "Warmup...";
        reduce_by_key_unchecked(ctx, elems_per_worker, seed, std::min(100, reps), true);

        auto test = [reps, elems_per_worker, seed, config_param, &ctx](
            auto config, const std::string& config_name) {
            if (config_name != config_param) {
                return;
            }
            RLOG << "Executing chosen configuration " << config_name;
            reduce_by_key(ctx, checkers::ReduceManipulatorDummy(), config,
                          "Dummy", config_name, elems_per_worker, seed, reps);
        };

        run_timings(test);

        if (config_param == "unchecked") {
            reduce_by_key_unchecked(ctx, elems_per_worker, seed, reps);
        }
    });
}

/******************************************************************************/
