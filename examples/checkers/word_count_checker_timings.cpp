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
const size_t default_words_per_worker = 125000;
const size_t default_distinct_words = 1000000;

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    int reps = default_reps;
    size_t words_per_worker = default_words_per_worker,
        distinct_words = default_distinct_words;
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('w', "words", words_per_worker, "words per worker");
    clp.add_size_t('d', "distinct", distinct_words, "number of distinct words");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run([&](Context& ctx) {
        ctx.enable_consume();
        // warmup
        word_count_unchecked(words_per_worker, distinct_words, 100, true)(ctx);

        auto test = [words_per_worker, distinct_words, reps, &ctx](
            auto config, const std::string& config_name) {
            word_count_factory(checkers::ReduceManipulatorDummy(), config,
                               "Dummy", config_name, words_per_worker,
                               distinct_words, reps)(ctx);
        };

        run_timings(test);

        word_count_unchecked(words_per_worker, distinct_words, reps)(ctx);
    });
}

/******************************************************************************/
