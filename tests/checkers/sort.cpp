/*******************************************************************************
 * tests/checkers/sort.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/checkers/sort.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

constexpr size_t default_reps = 100;

auto sort_random = [](auto manipulator, size_t reps = default_reps) {
    using Value = int;
    using Compare = std::less<Value>;
    using Checker = checkers::SortChecker<Value, Compare>;
    using Driver = checkers::Driver<Checker, decltype(manipulator)>;

    return [reps](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();

        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();

            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value {
                        return distribution(generator);
                    })
                .Sort(Compare{}, driver)
                .Size();

            ASSERT_TRUE(force_eval > 0); // dummy
            ASSERT_TRUE(driver->check(ctx));
        }
    };
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) TEST(Sort, SortWith ## MANIP) {               \
        api::Run(sort_random(checkers::SortManipulator ## MANIP()));    \
}

// run with specified number of iterations
#define TEST_CHECK_I(MANIP, ITS) TEST(Sort, SortWith ## MANIP) {              \
        api::Run(sort_random(checkers::SortManipulator ## MANIP(), ITS));   \
}

// run with template parameter
#define TEST_CHECK_T(NAME, FULL) TEST(Sort, SortWith ## NAME) {        \
        api::Run(sort_random(checkers::SortManipulator ## FULL()));    \
}

TEST_CHECK_I(Dummy, 1)
TEST_CHECK(DropLast)
TEST_CHECK(ResetToDefault)
TEST_CHECK(AddToEmpty)
TEST_CHECK(SetEqual)
TEST_CHECK(DuplicateLast)
TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>)

/******************************************************************************/
