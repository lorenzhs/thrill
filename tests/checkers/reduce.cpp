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
#include <thrill/api/size.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/reduce.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <random>

using namespace thrill; // NOLINT

constexpr size_t default_reps = 100;

auto reduce_by_key_test_factory = [](auto manipulator, size_t reps = default_reps) {
    using Value = size_t;
    using ReduceFn = std::plus<Value>;

    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn>;
    using Manipulator = decltype(manipulator);
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();

        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            auto key_extractor = [](Value in) { return in & 0xFFFF; };

            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .ReduceByKey(
                    VolatileKeyTag, key_extractor, ReduceFn(),
                    api::DefaultReduceConfig(), driver)
                .Size();

            ASSERT_TRUE(force_eval > 0); // dummy
            ASSERT_TRUE(driver->check(ctx));
        }
    };
};


// yikes, preprocessor
#define TEST_CHECK(MANIP) TEST(Reduce, ReduceByKeyWith ## MANIP) { \
        api::Run(reduce_by_key_test_factory(                       \
                     checkers::ReduceManipulator ## MANIP()));     \
}

TEST_CHECK(Dummy)
TEST_CHECK(DropFirst)
TEST_CHECK(IncFirst)
TEST_CHECK(IncFirstKey)
TEST_CHECK(SwitchValues)

/******************************************************************************/
