/*******************************************************************************
 * thrill/common/defines.hpp
 *
 * Define macros.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_DEFINES_HEADER
#define THRILL_COMMON_DEFINES_HEADER

#include <type_traits>

namespace thrill {
namespace common {

/******************************************************************************/
// LIKELY and UNLIKELY

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_LIKELY(c)   __builtin_expect((c), 1)
#define THRILL_UNLIKELY(c) __builtin_expect((c), 0)
#else
#define THRILL_LIKELY(c)   c
#define THRILL_UNLIKELY(c) c
#endif

/******************************************************************************/
// detect ThreadSanitizer

#ifndef THRILL_HAVE_THREAD_SANITIZER

#if defined(__has_feature)

// this works with clang
#if __has_feature(thread_sanitizer)
#define THRILL_HAVE_THREAD_SANITIZER 1
#else
#define THRILL_HAVE_THREAD_SANITIZER 0
#endif

#else

// gcc's sanitizers cannot be detected?
#define THRILL_HAVE_THREAD_SANITIZER 0

#endif

#endif  // THRILL_HAVE_THREAD_SANITIZER

/******************************************************************************/
// __attribute__ ((packed))

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_ATTRIBUTE_PACKED __attribute__ ((packed))
#else
#define THRILL_ATTRIBUTE_PACKED
#endif

/******************************************************************************/
// __attribute__ ((warn_unused_result))

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_ATTRIBUTE_WARN_UNUSED_RESULT __attribute__ ((warn_unused_result))
#else
#define THRILL_ATTRIBUTE_WARN_UNUSED_RESULT
#endif

/******************************************************************************/
// __attribute__ ((always_inline))

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_ATTRIBUTE_ALWAYS_INLINE __attribute__ ((always_inline)) inline
#else
#define THRILL_ATTRIBUTE_ALWAYS_INLINE inline
#endif

/******************************************************************************/
// __attribute__ ((format(printf, #, #))

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_ATTRIBUTE_FORMAT_PRINTF(X, Y) \
    __attribute__ ((format(printf, X, Y))) // NOLINT
#else
#define THRILL_ATTRIBUTE_FORMAT_PRINTF(X, Y)
#endif

/******************************************************************************/
// UNUSED(variable)

template <typename U>
void UNUSED(U&&) { }

/******************************************************************************/
// std::is_trivially_copyable<T> work-around for libstdc++ < 5.0

#if defined(__GLIBCXX__)
template <typename T>
struct is_trivially_copyable
    : std::integral_constant<bool, __has_trivial_copy(T)>{ };  // NOLINT
#else // GLIBCXX work-around
template <typename T>
using is_trivially_copyable = std::is_trivially_copyable<T>;
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DEFINES_HEADER

/******************************************************************************/
