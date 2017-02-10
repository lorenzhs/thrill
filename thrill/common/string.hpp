/*******************************************************************************
 * thrill/common/string.hpp
 *
 * Some string helper functions
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STRING_HEADER
#define THRILL_COMMON_STRING_HEADER

#include <thrill/common/defines.hpp>

#include <array>
#include <cstdarg>
#include <cstdlib>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/*!
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
std::string Hexdump(const void* const data, size_t size);

//! Dump a (binary) item as a sequence of hexadecimal pairs.
template <typename Type>
std::string HexdumpItem(const Type& t) {
    return Hexdump(&t, sizeof(t));
}

/*!
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
std::string Hexdump(const std::string& str);

/*!
 * Checks if the given match string is located at the start of this string.
 */
static inline
bool StartsWith(const std::string& str, const std::string& match) {
    if (match.size() > str.size()) return false;
    return std::equal(match.begin(), match.end(), str.begin());
}

/*!
 * Checks if the given match string is located at the end of this string.
 */
static inline
bool EndsWith(const std::string& str, const std::string& match) {
    if (match.size() > str.size()) return false;
    return std::equal(match.begin(), match.end(),
                      str.end() - match.size());
}

/*!
 * Helper for using sprintf to format into std::string and also to_string
 * converters.
 *
 * \param max_size maximum length of output string, longer ones are truncated.
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String str_snprintf(size_t max_size, const char* fmt, ...)
THRILL_ATTRIBUTE_FORMAT_PRINTF(2, 3);

template <typename String>
String str_snprintf(size_t max_size, const char* fmt, ...) {
    // allocate buffer on stack
    char* s = static_cast<char*>(alloca(max_size));

    va_list args;
    va_start(args, fmt);

    const int len = std::vsnprintf(s, max_size, fmt, args);

    va_end(args);

    return String(s, s + len);
}

/*!
 * Helper for using sprintf to format into std::string and also to_string
 * converters.
 *
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String str_sprintf(const char* fmt, ...)
THRILL_ATTRIBUTE_FORMAT_PRINTF(1, 2);

template <typename String>
String str_sprintf(const char* fmt, ...) {
    // allocate buffer on stack
    char* s = static_cast<char*>(alloca(256));

    va_list args;
    va_start(args, fmt);

    int len = std::vsnprintf(s, 256, fmt, args); // NOLINT

    if (len >= 256) {
        // try again.
        s = static_cast<char*>(alloca(len + 1));

        len = std::vsnprintf(s, len + 1, fmt, args);
    }

    va_end(args);

    return String(s, s + len);
}

//! Use ostream to output any type as string. You generally DO NOT want to use
//! this, instead create a larger ostringstream.
template <typename Type>
static inline
std::string to_str(const Type& t) {
    std::ostringstream oss;
    oss << t;
    return oss.str();
}

/*!
 * Template transformation function which uses std::istringstream to parse any
 * istreamable type from a std::string. Returns true only if the whole string
 * was parsed.
 */
template <typename Type>
static inline
bool from_str(const std::string& str, Type& outval) {
    std::istringstream is(str);
    is >> outval;
    return is.eof();
}

/******************************************************************************/
//! Number parsing helpers, wraps strto{f,d,ld,l,ul,ll,ull}() via type switch.

template <typename T>
T from_cstr(const char* nptr, char** endptr = nullptr, int base = 10);

/*----------------------------------------------------------------------------*/
// specializations for floating point types

// float
template <>
inline
float from_cstr<float>(const char* nptr, char** endptr, int) {
    return std::strtof(nptr, endptr);
}

// double
template <>
inline
double from_cstr<double>(const char* nptr, char** endptr, int) {
    return std::strtod(nptr, endptr);
}

// long double
template <>
inline
long double from_cstr<long double>(const char* nptr, char** endptr, int) {
    return std::strtold(nptr, endptr);
}

/*----------------------------------------------------------------------------*/
// specializations for integral types

// long
template <>
inline
long from_cstr<long>(const char* nptr, char** endptr, int base) {
    return std::strtol(nptr, endptr, base);
}
// unsigned long
template <>
inline
unsigned long from_cstr<unsigned long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoul(nptr, endptr, base);
}

// long long
template <>
inline
long long from_cstr<long long>(const char* nptr, char** endptr, int base) {
    return std::strtoll(nptr, endptr, base);
}
// unsigned long long
template <>
inline
unsigned long long from_cstr<unsigned long long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoull(nptr, endptr, base);
}

/******************************************************************************/
// Split and Join

/*!
 * Split the given string at each separator character into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str    string to split
 * \param sep    separator character
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string> Split(
    const std::string& str, char sep,
    std::string::size_type limit = std::string::npos);

void SplitRef(const std::string& str, char sep, std::vector<std::string>& vec);

/*!
 * Split the given string at each separator string into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str     string to split
 * \param sepstr  separator string, NOT a set of characters!
 * \param limit   maximum number of parts returned
 * \return        vector containing each split substring
 */
std::vector<std::string> Split(
    const std::string& str, const std::string& sepstr,
    std::string::size_type limit = std::string::npos);

//! Split a string by given separator string. Returns a vector of strings with
//! at least min_fields and at most limit_fields
std::vector<std::string>
Split(const std::string& str, const std::string& sep,
      unsigned int min_fields,
      unsigned int limit_fields = std::numeric_limits<unsigned int>::max());

/*!
 * Join a sequence of strings by some glue string between each pair from the
 * sequence. The sequence in given as a range between two iterators.
 *
 * \param glue  string to glue
 * \param first the beginning iterator of the range to join
 * \param last  the ending iterator of the range to join
 * \return      string constructed from the range with the glue between them.
 */
template <typename Iterator, typename Glue>
static inline
std::string Join(const Glue& glue, Iterator first, Iterator last) {
    std::ostringstream oss;
    if (first == last) return oss.str();

    oss << *first++;

    while (first != last) {
        oss << glue;
        oss << *first++;
    }

    return oss.str();
}

/*!
 * Join a Container (like a vector) of strings using some glue string between
 * each pair from the sequence.
 *
 * \param glue  string to glue
 * \param parts the vector of strings to join
 * \return      string constructed from the vector with the glue between them.
 */
template <typename Container, typename Glue>
static inline
std::string Join(const Glue& glue, const Container& parts) {
    return Join(glue, parts.begin(), parts.end());
}

//! Logging helper to print arrays as [a1,a2,a3,...]
template <typename T, size_t N>
static std::string VecToStr(const std::array<T, N>& data) {
    std::ostringstream oss;
    oss << '[';
    for (typename std::array<T, N>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) oss << ',';
        oss << *it;
    }
    oss << ']';
    return oss.str();
}

//! Logging helper to print vectors as [a1,a2,a3,...]
template <typename T>
static std::string VecToStr(const std::vector<T>& data) {
    std::ostringstream oss;
    oss << '[';
    for (typename std::vector<T>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) oss << ',';
        oss << *it;
    }
    oss << ']';
    return oss.str();
}

/*!
 * Replace all occurrences of needle in str. Each needle will be replaced with
 * instead, if found. The replacement is done in the given string and a
 * reference to the same is returned.
 *
 * \param str           the string to process
 * \param needle        string to search for in str
 * \param instead       replace needle with instead
 * \return              reference to str
 */
std::string& ReplaceAll(std::string& str, const std::string& needle,
                        const std::string& instead);

/*!
 * Trims the given string in-place on the left and right. Removes all
 * characters in the given drop array, which defaults to " \r\n\t".
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      reference to the modified string
 */
std::string& Trim(std::string& str, const std::string& drop = " \r\n\t");

/*!
 * Generate a random string of given length. The set of available
 * bytes/characters is given as the second argument. Each byte is equally
 * probable. Uses the pseudo-random number generator from stdlib; take care to
 * seed it using srand() before calling this function.
 *
 * \param size     length of result
 * \param rng      Random number generator to use, e.g. std::default_random_engine.
 * \param letters  character set to choose from
 * \return         random string of given length
 */
template <typename RandomEngine = std::default_random_engine>
static inline std::string
RandomString(std::string::size_type size, RandomEngine rng,
             const std::string& letters
                 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
    std::string out;
    out.resize(size);

    std::uniform_int_distribution<size_t> distribution(0, letters.size() - 1);

    for (size_t i = 0; i < size; ++i)
        out[i] = letters[distribution(rng)];

    return out;
}

//! Escape string using HTML entities
std::string EscapeHtml(const std::string& str);

//! Parse a string like "343KB" or "44 GiB" into the corresponding size in
//! bytes. Returns the number of bytes and sets ok = true if the string could
//! be parsed correctly. If no units indicator is given, use def_unit in
//! k/m/g/t/p (powers of ten) or in K/M/G/T/P (power of two).
bool ParseSiIecUnits(const char* str, uint64_t& size, char default_unit = 0);

//! Format a byte size using SI (K, M, G, T) suffixes (powers of ten). Returns
//! "123 M" or similar.
std::string FormatSiUnits(uint64_t number);

//! Format a byte size using IEC (Ki, Mi, Gi, Ti) suffixes (powers of
//! two). Returns "123 Ki" or similar.
std::string FormatIecUnits(uint64_t number);

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STRING_HEADER

/******************************************************************************/
