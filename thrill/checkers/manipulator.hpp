/*******************************************************************************
 * thrill/checkers/manipulator.hpp
 *
 * Common stuff for manipulators
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CHECKERS_MANIPULATOR_HEADER
#define THRILL_CHECKERS_MANIPULATOR_HEADER

#include <thrill/checkers/functional.hpp>
#include <thrill/common/meta.hpp>

namespace thrill {
namespace checkers {

//! Provides common manipulator functionality - don't use this, derive from it
struct ManipulatorBase : public noncopynonmove {
    bool made_changes() const { return made_changes_; }

    void reset() { made_changes_ = false; }

    //! Skip all items whose key is the default
    template <typename It>
    It skip_empty_key(It begin, It end) {
        using Key = typename std::iterator_traits<It>::value_type::first_type;
        while (begin < end && begin->first == Key()) ++begin;
        return begin;
    }
protected:
    bool made_changes_ = false;
};

//! Chain multiple manipulators for extra fun. Manipulators modify the input,
//! but don't return anything.
template <typename ... Manipulators>
struct ManipulatorStack { };

template <typename Manipulator>
struct ManipulatorStack<Manipulator>{
    template <typename ... Input>
    void operator () (Input& ... input) { manip(input ...); }

protected:
    Manipulator manip;
};

template <typename Manipulator, typename ... Next>
struct ManipulatorStack<Manipulator, Next ...>{
    template <typename ... Input>
    void operator () (Input& ... input) {
        manip(input ...);
        next(input ...);
    }

protected:
    Manipulator                manip;
    ManipulatorStack<Next ...> next;
};

//! Manipulator stack that returns something, which is then passed to the next
//! manipulator
template <typename ... Manipulators>
struct ManipulatorStackPass { };

template <typename Manipulator>
struct ManipulatorStackPass<Manipulator>{
    template <typename ... Input>
    auto operator () (Input ... input) { return manip(input ...); }

    bool made_changes() const { return manip.made_changes(); }

protected:
    Manipulator manip;
};

template <typename Manipulator, typename ... Next>
struct ManipulatorStackPass<Manipulator, Next ...>{
    template <typename ... Input>
    auto operator () (Input ... input) {
        return ApplyTuple(next, manip(input ...));
    }

    // Input was changes if any manipulator made a change.
    // Let's ignore the case where the changes cancel out...
    bool made_changes() const {
        return manip.made_changes() || next.made_changes();
    }

protected:
    Manipulator                    manip;
    ManipulatorStackPass<Next ...> next;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_MANIPULATOR_HEADER

/******************************************************************************/