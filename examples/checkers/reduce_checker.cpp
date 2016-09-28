/*******************************************************************************
 * examples/checkers/reduce_checker.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/size.hpp>
#include <thrill/checkers/driver.hpp>
#include <thrill/checkers/reduce.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <random>

using namespace thrill; // NOLINT

thread_local int my_rank = -1;

auto reduce_by_key_test_factory = [](auto manipulator, const std::string& name,
                                     size_t reps = 100) {
    using Value = size_t;
    using ReduceFn = std::plus<Value>;

    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn>;
    using Manipulator = decltype(manipulator);
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, name](Context& ctx) {
        std::default_random_engine generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 10000);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();

        sLOGC(my_rank == 0) << "Running" << name << "tests," << reps << "reps";

        size_t failures = 0, dummy = 0;
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

            dummy += force_eval;
            bool success = driver->check(ctx);

            if (!success) failures++;
        }

        LOGC(my_rank == 0)
            << name << ": " << failures << " out of " << reps
            << " tests failed";
    };
};

auto run = [](auto manipulator, const std::string& name, size_t reps = 100) {
    api::Run(reduce_by_key_test_factory(manipulator, name, reps));
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) run(checkers::ReduceManipulator ## MANIP(), #MANIP)

int main() {
    TEST_CHECK(Dummy);
    TEST_CHECK(DropFirst);
    TEST_CHECK(IncFirst);
    TEST_CHECK(IncFirstKey);
    TEST_CHECK(SwitchValues);
}

/******************************************************************************/
