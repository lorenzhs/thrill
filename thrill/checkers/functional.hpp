/*******************************************************************************
 * thrill/checkers/functional.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CHECKERS_FUNCTIONAL_HEADER
#define THRILL_CHECKERS_FUNCTIONAL_HEADER

namespace thrill {
namespace checkers {

/*!
 * Struct that signals whether the ReduceFunction is checkable.
 */
template <typename ReduceFunction>
struct reduce_checkable : public std::false_type { };

//! Addition is checkable
template <typename T>
struct reduce_checkable<std::plus<T> >: public std::true_type { };

//! Operations on a tuple member are checkable if the operation is
template <size_t Index, typename Tuple, typename Op>
struct reduce_checkable<common::TupleReduceIndex<Index, Tuple, Op> >
    : public reduce_checkable<Op>{ };

//! Convenience helper template for reduce_checkable
template <typename ReduceFunction>
constexpr bool reduce_checkable_v = reduce_checkable<ReduceFunction>::value;

template<typename T, typename = void>
struct is_iterator
{
   static constexpr bool value = false;
};

template<typename T>
struct is_iterator<T, typename std::enable_if_t<
                          !std::is_same<
                              typename std::iterator_traits<T>::value_type,
                              void>::value> >
{
   static constexpr bool value = true;
};

template <typename T>
constexpr bool is_iterator_v = is_iterator<T>::value;

struct noncopynonmove {
    //! default-constructible
    noncopynonmove() = default;
    //! non-copyable: delete copy-constructor
    noncopynonmove(const noncopynonmove&) = delete;
    //! non-copyable: delete assignment operator
    noncopynonmove& operator = (const noncopynonmove&) = delete;
    //! non-movable: delete move-constructor
    noncopynonmove(noncopynonmove&&) = delete;
    //! non-movable: delete move-assignment
    noncopynonmove& operator = (noncopynonmove&&) = delete;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_FUNCTIONAL_HEADER

/******************************************************************************/
