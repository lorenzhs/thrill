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
const size_t default_reps = 10000;
#else
const size_t default_reps = 100;
#endif

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    size_t reps = default_reps;
    clp.add_size_t('n', "iterations", reps, "iterations");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run(reduce_by_key_unchecked(reps));

    auto test = [reps](auto config, const std::string& config_name) {
        api::Run(reduce_by_key_test_factory(checkers::ReduceManipulatorDummy(),
                                            config, "Dummy", config_name, reps));
    };

    run_timings(test);
}

/******************************************************************************/
