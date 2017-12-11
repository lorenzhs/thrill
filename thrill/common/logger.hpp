/*******************************************************************************
 * thrill/common/logger.hpp
 *
 * Simple and less simple logging classes.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_LOGGER_HEADER
#define THRILL_COMMON_LOGGER_HEADER

#include <thrill/mem/allocator.hpp>
#include <thrill/mem/pool.hpp>
#include <tlx/meta/call_foreach_tuple.hpp>

#include <array>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace thrill {
namespace common {

namespace log {
// ANSI escape codes
constexpr auto reset() {
    return "\33[0m";
}
constexpr auto bold() {
    return "\33[1m";
}
constexpr auto underline() {
    return "\33[4m";
}
constexpr auto reverse() {
    return "\33[7m";
}
constexpr auto erase_line() {
    return "\33[K";
}
constexpr auto clear_screen() {
    return "\33[2J";
}

// foreground colours
constexpr auto fg_black() {
    return "\33[30m";
}
constexpr auto fg_red() {
    return "\33[31m";
}
constexpr auto fg_green() {
    return "\33[32m";
}
constexpr auto fg_yellow() {
    return "\33[33m";
}
constexpr auto fg_blue() {
    return "\33[34m";
}
constexpr auto fg_magenta() {
    return "\33[35m";
}
constexpr auto fg_cyan() {
    return "\33[36m";
}
constexpr auto fg_white() {
    return "\33[37m";
}

// background colours
constexpr auto bg_black() {
    return "\33[40m";
}
constexpr auto bg_red() {
    return "\33[41m";
}
constexpr auto bg_green() {
    return "\33[42m";
}
constexpr auto bg_yellow() {
    return "\33[43m";
}
constexpr auto bg_blue() {
    return "\33[44m";
}
constexpr auto bg_magenta() {
    return "\33[45m";
}
constexpr auto bg_cyan() {
    return "\33[46m";
}
constexpr auto bg_white() {
    return "\33[47m";
}

} // namespace log

//! memory manager singleton for Logger
extern mem::Manager g_logger_mem_manager;

//! Defines a name for the current thread.
void NameThisThread(const mem::by_string& name);

//! Returns the name of the current thread or 'unknown [id]'
std::string GetNameForThisThread();

/******************************************************************************/

/*!

\brief LOG and sLOG for development and debugging

This is a short description of how to use \ref LOG and \ref sLOG for rapid
development of modules with debug output, and how to **keep it afterwards**.

There are two classes Logger and SpacingLogger, but one does not use these
directly.

Instead there are the macros: \ref LOG and \ref sLOG that can be used as such:
\code
LOG << "This will be printed with a newline";
sLOG << "Print variables a" << a << "b" << b << "c" << c;
\endcode

There macros only print the lines if the boolean variable **debug** is
true. This variable is searched for in the scope of the LOG, which means it can
be set or overridden in the function scope, the class scope, from **inherited
classes**, or even the global scope.

\code
class MyClass
{
    static constexpr bool debug = true;

    void func1()
    {
        LOG << "Hello World";

        LOG0 << "This is temporarily disabled.";
    }

    void func2()
    {
        static constexpr bool debug = false;
        LOG << "This is not printed any more.";

        LOG1 << "But this is forced.";
    }
};
\endcode

There are two variation of \ref LOG and \ref sLOG : append 0 or 1 for
temporarily disabled or enabled debug lines. These macros are then \ref LOG0,
\ref LOG1, \ref sLOG0, and \ref sLOG1. The suffix overrides the debug variable's
setting.

After a module works as intended, one can just set `debug = false`, and all
debug output will disappear.

## Critique of LOG and sLOG

The macros are only for rapid module-based development. It cannot be used as an
extended logging system for our network framework, where logs of network
execution and communication are collected for later analysis. Something else is
needed here.

 */
class Logger
{
private:
    //! collector stream
    mem::safe_ostringstream oss_;

public:
    //! mutex synchronized output to std::cout
    static void Output(const char* str);
    //! mutex synchronized output to std::cout
    static void Output(const std::string& str);
    //! mutex synchronized output to std::cout
    static void Output(const mem::safe_string& str);

    Logger();

    //! output any type, including io manipulators
    template <typename AnyType>
    Logger& operator << (const AnyType& at) {
        oss_ << at;
        return *this;
    }

    //! destructor: output a newline
    ~Logger();
};

/*!
 * A logging class which outputs spaces between elements pushed via
 * operator<<. Depending on the real parameter the output may be suppressed.
 */
class SpacingLogger
{
private:
    //! true until the first element it outputted.
    bool first_ = true;

    //! collector stream
    mem::safe_ostringstream oss_;

public:
    SpacingLogger();

    //! output any type, including io manipulators
    template <typename AnyType>
    SpacingLogger& operator << (const AnyType& at) {
        if (!first_) oss_ << ' ';
        else first_ = false;

        oss_ << at;

        return *this;
    }

    //! destructor: output a newline
    ~SpacingLogger();
};

class LoggerVoidify
{
public:
    void operator & (Logger&) { }
    void operator & (SpacingLogger&) { }
};

//! Explicitly specify the condition for logging
#define LOGC(cond)      \
    !(cond) ? (void)0 : \
    ::thrill::common::LoggerVoidify() & ::thrill::common::Logger()

//! Default logging method: output if the local debug variable is true.
#define LOG LOGC(debug)

//! Override default output: never or always output log.
#define LOG0 LOGC(false)
#define LOG1 LOGC(true)

//! Explicitly specify the condition for logging
#define sLOGC(cond)     \
    !(cond) ? (void)0 : \
    ::thrill::common::LoggerVoidify() & ::thrill::common::SpacingLogger()

//! Default logging method: output if the local debug variable is true.
#define sLOG sLOGC(debug)

//! Override default output: never or always output log.
#define sLOG0 sLOGC(false)
#define sLOG1 sLOGC(true)

} // namespace common

namespace mem {

/******************************************************************************/
// Nice LOG/sLOG Formatters for std::pair, std::tuple, std::vector, and
// std::array types

using log_stream = safe_ostringstream;

template <typename A, typename B>
log_stream& operator << (log_stream& os, const std::pair<A, B>& p) {
    os << '(' << p.first << ',' << p.second << ')';
    return os;
}

template <typename Tuple, size_t N>
struct LogStreamTuplePrinter {
    static void print(log_stream& os, const Tuple& t) {
        LogStreamTuplePrinter<Tuple, N - 1>::print(os, t);
        os << ',' << std::get<N - 1>(t);
    }
};

template <typename Tuple>
struct LogStreamTuplePrinter<Tuple, 1>{
    static void print(log_stream& os, const Tuple& t) {
        os << std::get<0>(t);
    }
};

template <typename Tuple>
struct LogStreamTuplePrinter<Tuple, 0>{
    static void print(log_stream&, const Tuple&) { }
};

template <typename... Args>
log_stream& operator << (log_stream& os, const std::tuple<Args...>& t) {
    os << '(';
    LogStreamTuplePrinter<std::tuple<Args...>, sizeof ... (Args)>::print(os, t);
    os << ')';
    return os;
}

//! Logging helper to print arrays as [a1,a2,a3,...]
template <typename T, size_t N>
log_stream& operator << (log_stream& os, const std::array<T, N>& data) {
    os << '[';
    for (typename std::array<T, N>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) os << ',';
        os << *it;
    }
    os << ']';
    return os;
}

//! Logging helper to print vectors as [a1,a2,a3,...]
template <typename T>
log_stream& operator << (log_stream& os, const std::vector<T>& data) {
    os << '[';
    for (typename std::vector<T>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) os << ',';
        os << *it;
    }
    os << ']';
    return os;
}

} // namespace mem
} // namespace thrill

#endif // !THRILL_COMMON_LOGGER_HEADER

/******************************************************************************/
