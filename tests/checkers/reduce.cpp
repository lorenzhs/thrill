/*******************************************************************************
 * tests/checkers/reduce.cpp
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
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/reduce.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

using Manipulator = checkers::ReduceManipulatorDummy;

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

            auto driver = std::make_shared<
                checkers::Driver<
                    checkers::ReduceChecker<size_t, size_t, std::plus<size_t> >,
                    Manipulator>
                >();

            auto reduced = integers.ReduceByKey(
                VolatileKeyTag, modulo_two, std::plus<size_t>(),
                api::DefaultReduceConfig(), driver);

            auto force_eval = reduced.Size();

            ASSERT_TRUE(driver->check(ctx));
            ASSERT_TRUE(force_eval > 0);
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

            auto driver = std::make_shared<
                checkers::Driver<
                    checkers::ReduceChecker<size_t, size_t, std::plus<size_t> >,
                    Manipulator>
                >();

            auto reduced = integers.ReducePair(
                std::plus<size_t>(), api::DefaultReduceConfig(), driver);

            auto force_eval = reduced.Size();

            ASSERT_TRUE(driver->check(ctx));
            ASSERT_TRUE(force_eval > 0);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
