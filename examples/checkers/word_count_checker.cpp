/*******************************************************************************
 * examples/checkers/word_count_checker.cpp
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
#include "accuracy.hpp"

#ifdef CHECKERS_FULL
const int default_reps = 10000;
#else
const int default_reps = 100;
#endif
const size_t default_words_per_worker = 125000;
const size_t default_distinct_words = 1000000;

// yikes, preprocessor
#define TEST_CHECK(MANIP) if (run_ ## MANIP)                            \
        run_accuracy(ctx, word_count,                                   \
                     checkers::ReduceManipulator ## MANIP(),            \
                     #MANIP, words_per_worker, distinct_words, seed, reps)
#define TEST_CHECK_I(MANIP, ITS) if (run_ ## MANIP)                     \
        run_accuracy(ctx, word_count,                                   \
                     checkers::ReduceManipulator ## MANIP(),            \
                     #MANIP, words_per_worker, distinct_words, seed, ITS)
// run with template parameter
#define TEST_CHECK_T(NAME, FULL) if (run_ ## NAME)                      \
        run_accuracy(ctx, word_count,                                   \
                     checkers::ReduceManipulator ## FULL(),             \
                     #NAME, words_per_worker, distinct_words, seed, reps)

int main(int argc, char** argv) {
    tlx::CmdlineParser clp;

    int reps = default_reps;
    size_t words_per_worker = default_words_per_worker,
        distinct_words = default_distinct_words,
        seed = 42;
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('w', "words", words_per_worker, "words per worker");
    clp.add_size_t('d', "distinct", distinct_words, "number of distinct words");
    clp.add_size_t('e', "seed", seed, "seed for input generation (0: random)");

    bool run_RandFirstKey = false, run_SwitchValues = false,
        run_Bitflip = false, run_IncDec1 = false, run_IncDec2 = false,
        run_IncDec4 = false, run_IncDec8 = false, run_IncFirstKey = false;
    clp.add_flag('r', "RandFirstKey", run_RandFirstKey, "run RandFirstKey manip");
    clp.add_flag('s', "SwitchValues", run_SwitchValues, "run SwitchValues manip");
    clp.add_flag('b', "Bitflip", run_Bitflip, "run Bitflip manip");
    clp.add_flag('1', "IncDec1", run_IncDec1, "run IncDec1 manip");
    clp.add_flag('2', "IncDec2", run_IncDec2, "run IncDec2 manip");
    clp.add_flag('4', "IncDec4", run_IncDec4, "run IncDec4 manip");
    clp.add_flag('8', "IncDec8", run_IncDec8, "run IncDec8 manip");
    clp.add_flag('i', "IncFirstKey", run_IncFirstKey, "run IncFirstKey manip");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    api::Run([&](Context &ctx){
        ctx.enable_consume();
        my_rank = ctx.net.my_rank();
        // warmup
        word_count_unchecked(ctx, words_per_worker, distinct_words, seed, 10, true);

        TEST_CHECK(RandFirstKey);
        TEST_CHECK(SwitchValues);
        TEST_CHECK(Bitflip);
        TEST_CHECK_T(IncDec1, IncDec<1>);
        TEST_CHECK_T(IncDec2, IncDec<2>);
        TEST_CHECK_T(IncDec4, IncDec<4>);
        TEST_CHECK_T(IncDec8, IncDec<8>);
        // TEST_CHECK(DropFirst); // disabled because always detected
        // TEST_CHECK(IncFirst); // disabled because always detected
        // TEST_CHECK(RandFirst); // disabled because always detected
        TEST_CHECK(IncFirstKey);
    });
}

/******************************************************************************/
