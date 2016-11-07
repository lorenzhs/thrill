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

const size_t default_reps = 100;

thread_local static int my_rank = -1;

#define RLOG LOGC(my_rank == 0)
#define sRLOG sLOGC(my_rank == 0)

auto sort_random = [](const auto &manipulator, const std::string& name,
                      size_t reps) {
    using Value = int;
    using Compare = std::less<Value>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Checker = checkers::SortChecker<Value, Compare>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, name](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();

        sRLOG << "Running" << name << "tests," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();

            // Synchronize with barrier
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .Sort(Compare{}, driver)
                .Size();
            run_timer.Stop();
            dummy += force_eval;

            // Re-synchronize, then run final checking pass
            ctx.net.Barrier();
            check_timer.Start();
            auto success = driver->check(ctx);
            check_timer.Stop();

            if (!success.first) failures++;
            if (success.second) manips++;
        }

        RLOG << name << ": " << failures << " out of " << reps
             << " tests failed; " << manips << " manipulations";

        sRLOG
            << "Sort:" << run_timer.Microseconds()/(1000.0*reps)
            << "ms; Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms";
    };
};


auto sort_unchecked = [](size_t reps = 100) {
    using Value = int;
    using Compare = std::less<Value>;

    return [reps](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();

        sRLOG << "Running tests without checker," << reps << "reps";

        common::StatsTimerStopped run_timer;
        size_t dummy = 0;
        for (size_t i = 0; i < reps; ++i) {
            // Synchronize with barrier
            ctx.net.Barrier();
            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .Sort(Compare{})
                .Size();
            run_timer.Stop();
            dummy += force_eval;
        }

        sRLOG << "Sort:" << run_timer.Microseconds()/(1000.0*reps)
              << "ms (no checking, no manipulation)";
    };
};


auto run = [](const auto &manipulator, const std::string &name,
              size_t reps = default_reps) {
    api::Run(sort_random(manipulator, name, reps));
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) run(checkers::SortManipulator ## MANIP(), #MANIP)
#define TEST_CHECK_A(MANIP, ...) run(checkers::SortManipulator ## MANIP(), #MANIP, __VA_ARGS__)

// run with template parameter
#define TEST_CHECK_T(NAME, FULL) run(checkers::SortManipulator ## FULL(), #NAME)

int main() {
    api::Run(sort_unchecked(default_reps));
    TEST_CHECK(Dummy);
    TEST_CHECK(DropLast);
    TEST_CHECK(ResetToDefault);
    TEST_CHECK(AddToEmpty);
    TEST_CHECK(SetEqual);
    TEST_CHECK(DuplicateLast);
    TEST_CHECK_T(MoveToNextBlock, MoveToNextBlock<int>);
}

/******************************************************************************/
