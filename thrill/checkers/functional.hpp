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

#include <thrill/common/functional.hpp>
#include <tlx/meta/log2.hpp>

#include <limits>
#include <ostream>
#include <sstream>
#include <type_traits>

namespace thrill {
namespace checkers {

template <typename Integral>
struct checked_plus {
    static constexpr bool debug = false;

    Integral inline __attribute__ ((always_inline))
    operator () (const Integral& i1, const Integral& i2) const {
        Integral result;
        if (TLX_UNLIKELY(__builtin_add_overflow(i1, i2, &result))) {
            sLOG << "Add overflow:" << i1 << "+" << i2 << "!=" << result;
            result = (i1 % modulus) + (i2 % modulus);
        }
        return result;
    }
    Integral modulus = std::numeric_limits<Integral>::max();
};

/*!
 * Struct that signals whether the ReduceFunction is checkable.
 */
template <typename ReduceFunction>
struct reduce_checkable : public std::false_type { };

//! Addition is checkable
template <typename T>
struct reduce_checkable<std::plus<T> >: public std::true_type { };

template <typename T>
struct reduce_checkable<checked_plus<T> >: public std::true_type { };

//! Operations on a tuple member are checkable if the operation is
template <size_t Index, typename Tuple, typename Op>
struct reduce_checkable<common::TupleReduceIndex<Index, Tuple, Op> >
    : public reduce_checkable<Op>{ };

//! Convenience helper template for reduce_checkable
template <typename ReduceFunction>
constexpr bool reduce_checkable_v = reduce_checkable<ReduceFunction>::value;

/*!
 * Struct that signals whether the ReduceFunction supports builtin modulo
 */
template <typename ReduceFunction>
struct reduce_modulo_builtin : public std::false_type { };

//! Checked addition includes the modulo
template <typename T>
struct reduce_modulo_builtin<checked_plus<T> >: public std::true_type { };

//! Operations on a tuple member do iff the operation does
template <size_t Index, typename Tuple, typename Op>
struct reduce_modulo_builtin<common::TupleReduceIndex<Index, Tuple, Op> >
    : public reduce_modulo_builtin<Op>{ };

//! Convenience helper template for reduce_modulo_builtin
template <typename ReduceFunction>
constexpr bool reduce_modulo_builtin_v =
    reduce_modulo_builtin<ReduceFunction>::value;

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

//! Type trait to check whether a type can be printed to an std::ostream
template <typename T, typename Result = void>
struct is_printable : std::false_type { };

template <typename T>
struct is_printable<T,
    typename std::enable_if_t<
        // check convertability of...
        std::is_convertible<
            // the return type of printing a T to an std::ostream...
            decltype(std::declval<std::ostream&>() << std::declval<T const&>()),
            // to an std::ostream& (to match the signature of op<<)
            std::ostream&
        >::value>
    > : std::true_type {};

//! Convert a type to a string where this is possible
template <typename T>
typename std::enable_if_t<is_printable<T>::value, std::string>
maybe_print(T const& t) {
    std::ostringstream out;
    out << t;
    return out.str();
}

//! Fallback for non-printable types, prints "✖"
template <typename T>
typename std::enable_if_t<!is_printable<T>::value, std::string>
maybe_print(T const&) {
    return "✖";
}

template<size_t bytes> struct select_uint_;
template<> struct select_uint_<1> { using type = uint8_t; };
template<> struct select_uint_<2> { using type = uint16_t; };
template<> struct select_uint_<4> { using type = uint32_t; };
template<> struct select_uint_<8> { using type = uint64_t; };

template <unsigned long long max>
struct select_uint : select_uint_<((tlx::Log2<max>::ceil + 7) >> 3)>{ };

template <unsigned long long max>
using select_uint_t = typename select_uint<max>::type;

template <typename Integer>
constexpr bool is_power_of_two(Integer x) {
    return x && ((x & (x - 1)) == 0);
}

//! Template if, mainly for static constexpr foo = if_<cond, foo, bar>::value;
template <bool cond, auto if_true, auto if_false>
struct if_ {
    static_assert(std::is_same_v<decltype(if_true), decltype(if_false)>,
                  "if_true and if_false must be of the same type");
    static constexpr auto value = if_false;
};

template <auto if_true, auto if_false>
struct if_<true, if_true, if_false>{
    static_assert(std::is_same_v<decltype(if_true), decltype(if_false)>,
                  "if_true and if_false must be of the same type");
    static constexpr auto value = if_true;
};

template <bool cond, auto if_true, auto if_false>
constexpr auto if_v = if_<cond, if_true, if_false>::value;

// Compile time exponentiation (pow isn't constexpr)
template<size_t n>
struct pow_helper{
    static constexpr double pow(const double& a){
        return a * pow_helper<n-1>::pow(a);
    }
};

//final specialization pow
template<>
struct pow_helper<0>{
    static constexpr double pow(const double& /* unused */){
        return 1.0;
    }
};


// from https://stackoverflow.com/a/39348287/3793885
template<class X, class Y, class Op>
struct op_valid_impl
{
    template<class U, class L, class R>
    static auto test(int) -> decltype(std::declval<U>()(std::declval<L>(), std::declval<R>()),
                                      void(), std::true_type());

    template<class U, class L, class R>
    static auto test(...) -> std::false_type;

    using type = decltype(test<Op, X, Y>(0));

};

template<class X, class Y, class Op>
using op_valid = typename op_valid_impl<X, Y, Op>::type;

template<class X, class Y>
using has_equal = op_valid<X, Y, std::equal_to<>>;


} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_FUNCTIONAL_HEADER

/******************************************************************************/
