/*******************************************************************************
 * tests/core/reduce_checker_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

TEST(ReduceChecker, ReduceModulo2CorrectResults) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 0x1000000,
                [](const size_t& index) {
                    return index + 1;
                });

            auto modulo_two = [](size_t in) {
                                  return (in % 2) + 1;
                              };

            auto add_function = std::plus<size_t>();
            auto reduced = integers.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function);

            std::vector<size_t> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            size_t i = 0;

            for (const size_t& element : out_vec) {
                ASSERT_EQ(element, (1ULL << 46) + (1ULL << 23) * (i++));
            }

            ASSERT_EQ((size_t)2, out_vec.size());

        };

    api::RunLocalTests(start_func);
}

//! Test sums of integers 0..n-1 for n=100 in 1000 buckets in the reduce table
TEST(ReduceChecker, ReduceModuloPairsCorrectResults) {

    static constexpr size_t test_size = 0x1000000;
    static constexpr size_t mod_size = 1024u;
    static constexpr size_t div_size = test_size / mod_size;

    auto start_func =
        [](Context& ctx) {

            using IntPair = std::pair<size_t, size_t>;

            auto integers = Generate(
                ctx, test_size,
                [](const size_t& index) {
                    return IntPair(index % mod_size, index / mod_size);
                });

            auto add_function = std::plus<size_t>();
            auto reduced = integers.ReducePair(add_function);

            std::vector<IntPair> out_vec = reduced.AllGather();

            std::sort(out_vec.begin(), out_vec.end(),
                      [](const IntPair& p1, const IntPair& p2) {
                          return p1.first < p2.first;
                      });

            ASSERT_EQ(mod_size, out_vec.size());
            for (const auto& element : out_vec) {
                ASSERT_EQ(element.second, (div_size * (div_size - 1)) / 2u);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
