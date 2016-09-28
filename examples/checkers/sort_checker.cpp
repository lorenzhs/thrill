/*******************************************************************************
 * examples/checkers/sort_checker.cpp
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
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/sort.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <utility>

using namespace thrill; // NOLINT

thread_local static int my_rank = -1;

auto sort_random = [](auto manipulator, const std::string& name,
                      size_t reps = 100) {
    using Value = int;
    using Compare = std::less<Value>;
    using Checker = checkers::SortChecker<Value, Compare>;
    using Driver = checkers::Driver<Checker, decltype(manipulator)>;

    return [reps, name](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();

        sLOGC(my_rank == 0) << "Running" << name << "tests," << reps << "reps";

        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();

            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .Sort(Compare{}, driver)
                .Size();

            dummy += force_eval;
            auto success = driver->check(ctx);

            if (!success.first) failures++;
            if (success.second) manips++;
        }

        LOGC(my_rank == 0)
            << name << ": " << failures << " out of " << reps
            << " tests failed; " << manips << " manipulations";
    };
};

auto run = [](auto manipulator, const std::string &name, size_t reps = 100) {
    api::Run(sort_random(manipulator, name, reps));
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) run(checkers::SortManipulator ## MANIP(), #MANIP)
#define TEST_CHECK_A(MANIP, ...) run(checkers::SortManipulator ## MANIP(), #MANIP, __VA_ARGS__)

// run with template parameter
#define TEST_CHECK_T(NAME, FULL) run(checkers::SortManipulator ## FULL(), #NAME)

int main(int argc, char** argv) {
    TEST_CHECK_A(Dummy, 1);
    TEST_CHECK(DropLast);
    TEST_CHECK(ResetToDefault);
    TEST_CHECK(AddToEmpty);
    TEST_CHECK(SetEqual);
    TEST_CHECK(DuplicateLast);
    TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>);
}

/******************************************************************************/
