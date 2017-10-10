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
    size_t elems_per_worker = default_elems_per_worker;
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('e', "elems", elems_per_worker, "elements per worker");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run([&](Context& ctx){
        ctx.enable_consume();
        // warmup
        reduce_by_key_unchecked(elems_per_worker, 100, true)(ctx);

        auto test = [reps, elems_per_worker, &ctx](
            auto config, const std::string& config_name) {
            reduce_by_key_test_factory(checkers::ReduceManipulatorDummy(),
                                       config, "Dummy", config_name,
                                       elems_per_worker, reps)(ctx);
        };

        run_timings(test);

        reduce_by_key_unchecked(elems_per_worker, reps)(ctx);
    });
}

/******************************************************************************/
