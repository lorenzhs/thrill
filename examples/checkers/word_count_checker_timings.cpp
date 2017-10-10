/*******************************************************************************
 * examples/checkers/word_count_checker_timings.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <tlx/cmdline_parser.hpp>

#include "word_count.hpp"
#include "timings.hpp"

#ifdef CHECKERS_FULL
const int default_reps = 10000;
#else
const int default_reps = 100;
#endif
const size_t default_num_words = 1000000;

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    int reps = default_reps;
    size_t num_words = default_num_words;
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('w', "words", num_words, "num_words");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run([&](Context& ctx) {
        ctx.enable_consume();
        // warmup
        word_count_unchecked(num_words, 10, true)(ctx);

        word_count_unchecked(num_words, reps)(ctx);

        auto test = [num_words, reps, &ctx](auto config,
                                            const std::string& config_name) {
            word_count_factory(checkers::ReduceManipulatorDummy(), config,
                               "Dummy", config_name, num_words, reps)(ctx);
        };

        run_timings(test);
    });
}

/******************************************************************************/
