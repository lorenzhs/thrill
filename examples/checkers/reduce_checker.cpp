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
#include <thrill/common/stats_timer.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <random>

using namespace thrill; // NOLINT

thread_local int my_rank = -1;

auto reduce_by_key_test_factory = [](const auto &manipulator,
                                     const std::string& name,
                                     size_t reps) {
    using Value = size_t;
    using ReduceFn = std::plus<Value>;

    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, name](Context& ctx) {
        std::mt19937 generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();


        sLOGC(my_rank == 0) << "Running ReduceByKey tests with" << name
                            << "manipulator," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();
            auto key_extractor = [](const Value& in) { return in & 0xFFFF; };

            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Value
                    { return distribution(generator); })
                .ReduceByKey(
                    VolatileKeyTag, key_extractor, ReduceFn(),
                    api::DefaultReduceConfig(), driver)
                .Size();
            run_timer.Stop();

            dummy += force_eval;
            check_timer.Start();
            auto success = driver->check(ctx);
            check_timer.Stop();

            if (!success.first) failures++;
            if (success.second) manips++;
        }

        LOGC(my_rank == 0)
            << "ReduceByKey with " << name << " manipulator: "
            << (failures > 0 ? common::log::fg_red() : "")
            << failures << " out of " << reps << " tests failed"
            << "; " << manips << " manipulations"
            << common::log::reset();

        sLOGC(my_rank == 0)
            << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
            << "ms; Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms";
    };
};

auto reduce_pair_test_factory = [](const auto &manipulator,
                                   const std::string& name,
                                   size_t reps) {
    using Value = size_t;
    using Pair = std::pair<Value, Value>;
    using ReduceFn = std::plus<Value>;

    using Checker = checkers::ReduceChecker<Value, Value, ReduceFn>;
    using Manipulator = std::decay_t<decltype(manipulator)>;
    using Driver = checkers::Driver<Checker, Manipulator>;

    return [reps, name](Context& ctx) {
        std::mt19937 generator(std::random_device { } ());
        std::uniform_int_distribution<Value> distribution(0, 0xFFFFFFFF);

        ctx.enable_consume();
        if (my_rank < 0) my_rank = ctx.net.my_rank();

        sLOGC(my_rank == 0) << "Running ReducePair tests with" << name
                            << "manipulator," << reps << "reps";

        common::StatsTimerStopped run_timer, check_timer;
        size_t failures = 0, dummy = 0, manips = 0;
        for (size_t i = 0; i < reps; ++i) {
            auto driver = std::make_shared<Driver>();
            driver->silence();

            run_timer.Start();
            size_t force_eval =
                Generate(
                    ctx, 1000000,
                    [&distribution, &generator](const size_t&) -> Pair {
                        return Pair{distribution(generator) & 0xFFFF, // key
                                distribution(generator)};
                    })
                .ReducePair(ReduceFn(), api::DefaultReduceConfig(), driver)
                .Size();
            run_timer.Stop();

            dummy += force_eval;
            check_timer.Start();
            auto success = driver->check(ctx);
            check_timer.Stop();

            if (!success.first) failures++;
            if (success.second) manips++;
        }

        LOGC(my_rank == 0)
            << "ReducePair with " << name << " manipulator: "
            << (failures > 0 ? common::log::fg_red() : "")
            << failures << " out of " << reps << " tests failed"
            << "; " << manips << " manipulations"
            << common::log::reset();

        sLOGC(my_rank == 0)
            << "Reduce:" << run_timer.Microseconds()/(1000.0*reps)
            << "ms; Check:" << check_timer.Microseconds()/(1000.0*reps) << "ms";
    };
};


auto run = [](const auto &manipulator, const std::string& name,
              size_t reps = 100) {
    api::Run(reduce_by_key_test_factory(manipulator, name, reps));
    //api::Run(reduce_pair_test_factory(manipulator, name, reps));
};

// yikes, preprocessor
#define TEST_CHECK(MANIP) run(checkers::ReduceManipulator ## MANIP(), #MANIP)
#define TEST_CHECK_I(MANIP, ITS) run(checkers::ReduceManipulator ## MANIP(), #MANIP, ITS)

namespace std {
template <typename T, typename U>
ostream& operator << (ostream& os, const pair<T, U>& p) {
    return os << '(' << p.first << ',' << p.second << ')';
}
}

int main() {
    TEST_CHECK(Dummy);
    TEST_CHECK(RandFirstKey);
    TEST_CHECK(SwitchValues);
    TEST_CHECK(DropFirst);
    TEST_CHECK(IncFirst);
    TEST_CHECK(RandFirst);
    TEST_CHECK(IncFirstKey);
}

/******************************************************************************/
