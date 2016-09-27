/*******************************************************************************
 * thrill/checkers/driver.hpp
 *
 * "Test driver" to supervise checking and manipulation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CHECKERS_DRIVER_HEADER
#define THRILL_CHECKERS_DRIVER_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>

#include <type_traits>

namespace thrill {
namespace checkers {

template <typename Checker, typename Manipulator>
class Driver
{
    static const bool debug = true;

public:
    template <typename C = Checker>
    Driver(typename std::enable_if_t<
           std::is_default_constructible<Checker>::value>* = 0)
        : checker_(), manipulator_() {}

    template <typename Arg>
    explicit Driver(Arg &&arg, typename std::enable_if_t<
                    std::is_constructible<Checker, Arg&&>::value>* = 0)
        : checker_(std::forward<Arg>(arg)), manipulator_() {}

    Driver(Checker checker, Manipulator manipulator)
        : checker_(checker),
          manipulator_(manipulator)
    { }

    using checker_t = Checker;
    using manipulator_t = Manipulator;

    void reset() {
        checker_.reset();
        manipulator_.reset();
    }

    bool check(api::Context& ctx) {
        bool success = checker_.check(ctx);
        bool manipulated = manipulator_.made_changes();

        // We need to check whether a manipulation was made on *any* worker
        int manipulated_count = ctx.net.AllReduce((int)manipulated);
        manipulated = (manipulated_count > 0);

        sLOGC(debug && ctx.net.my_rank() == 0)
            << "checking driver: check" << success << "manip" << manipulated;

        // If it was manipulated and detected, or not manipulated and passed,
        // then we're good
        return (success == !manipulated);
    }

    Checker& checker() { return checker_; }
    Manipulator& manipulator() { return manipulator_; }

protected:
    Checker checker_;
    Manipulator manipulator_;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_DRIVER_HEADER

/******************************************************************************/
