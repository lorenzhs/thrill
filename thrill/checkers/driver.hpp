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

namespace thrill {
namespace checkers {

template <typename Checker, typename Manipulator>
class driver {
    static const bool debug = true;
public:
    driver(Checker &checker, Manipulator &manipulator)
        : checker_(checker)
        , manipulator_(manipulator)
    {}

    bool check(api::Context& ctx) {
        bool success = checker_.check(ctx);
        bool manipulated = manipulator_.made_changes();

        sLOG << "checking driver: checker" << success
             << " manipulated" << manipulated;

        // If it was manipulated and detected, or not manipulated and passed,
        // then we're good
        return (success == !manipulated);
    }

    Checker& checker() { return checker_; }
    Manipulator& manipulator() { return manipulator_; }

protected:
    Checker& checker_;
    Manipulator& manipulator_;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_DRIVER_HEADER

/******************************************************************************/
