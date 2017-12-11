/*******************************************************************************
 * examples/checkers/word_count_checker_only.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <tlx/cmdline_parser.hpp>

constexpr int loop_fct = 100;
constexpr int warmup_its = 10; // more warmup

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
        distinct_words = default_distinct_words,
        seed = 42;
    std::string config_param = "8x16_CRC32_m15";
    clp.add_int('n', "iterations", reps, "iterations");
    clp.add_size_t('w', "words", words_per_worker, "words per worker");
    clp.add_size_t('d', "distinct", distinct_words, "number of distinct words");
    clp.add_size_t('s', "seed", seed, "seed for input generation (0: random)");
    clp.add_string('c', "config", config_param, "which configuration to run");

    if (!clp.process(argc, argv)) return -1;
    clp.print_result();

    if (std::find(known_configs.begin(), known_configs.end(), config_param) ==
        known_configs.end()) {
        LOG1 << "unknown config: " << config_param;
        return 1;
    }

    auto test = [words_per_worker, distinct_words, seed, reps, config_param](
        auto config, const std::string& config_name) {
        if (config_name != config_param) {
            return;
        }
        word_count_checkonly(config, config_name, words_per_worker,
                             distinct_words, seed, reps);
    };

    run_timings(test);

}

/******************************************************************************/
