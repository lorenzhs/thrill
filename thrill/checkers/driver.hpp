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

#include <utility>

namespace thrill {
namespace checkers {

template <typename Checker, typename Manipulator>
class Driver
{
    static const bool debug = false;

public:
    template <typename ... Args>
    explicit Driver(Args&& ... args)
        : checker_(std::forward<Args>(args) ...) { }

    Driver(const Driver&) = delete;
    Driver(Driver&&) = delete;

    using checker_t = Checker;
    using manipulator_t = Manipulator;

    void reset() {
        checker_.reset();
        manipulator_.reset();
    }

    void silence() { silent = true; }
    void unsilence() { silent = false; }

    std::pair<bool, bool> check(api::Context& ctx) {
        bool success = checker_.check(ctx);
        bool manipulated = manipulator_.made_changes();

        // We need to check whether a manipulation was made on *any* worker
        int manipulated_count = ctx.net.AllReduce((int)manipulated);
        manipulated = (manipulated_count > 0);

        sLOGC(debug && ctx.net.my_rank() == 0)
            << "checking driver: check" << success << "manip" << manipulated;

        LOGC(!silent && success == manipulated && ctx.net.my_rank() == 0)
            << common::log::bold() << common::log::fg_red()
            << "Checker failure: check " << success << "; manip " << manipulated
            << common::log::reset();

        // If it was manipulated and detected, or not manipulated and passed,
        // then we're good
        return std::make_pair(success == !manipulated, manipulated);
    }

    Checker& checker() { return checker_; }
    Manipulator& manipulator() { return manipulator_; }

protected:
    Checker checker_;
    Manipulator manipulator_;
    bool silent = false;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_DRIVER_HEADER

/******************************************************************************/
