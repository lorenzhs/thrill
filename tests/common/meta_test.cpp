/*******************************************************************************
 * tests/common/meta_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/meta.hpp>

#include <gtest/gtest.h>

#include <sstream>
#include <tuple>

using namespace thrill;

std::ostringstream g_oss;

/******************************************************************************/
// VariadicCallForeach

std::tuple<int, char, double> my_tuple {
    1, '2', 3
};

struct DoSomething {
    template <typename Index, typename Arg>
    void operator () (Index index, const Arg& a) const {
        g_oss << index << " " << a << " "
              << std::get<Index::index>(my_tuple) << "\n";
    }
};

template <typename ... Args>
void func(const Args& ... args) {
    common::VariadicCallForeachIndex(
        [](auto index, auto a) {
            g_oss << index << " " << a << " "
                  << std::get<decltype(index)::index>(my_tuple) << "\n";
        },
        args ...);

    common::VariadicCallForeachIndex(DoSomething(), args ...);

    // due to automatic conversion, we can just take a size_t, too.
    common::VariadicCallForeachIndex(
        [](size_t index, auto a) {
            g_oss << index << " " << a << "\n";
        },
        args ...);
}

TEST(Meta, VariadicCallForeach) {
    g_oss.str("");

    func(static_cast<int>(42),
         static_cast<double>(5),
         "hello");

    ASSERT_EQ("0 42 1\n1 5 2\n2 hello 3\n"
              "0 42 1\n1 5 2\n2 hello 3\n"
              "0 42\n1 5\n2 hello\n", g_oss.str());
}

/******************************************************************************/
// VariadicCallEnumerate

TEST(Meta, VariadicCallEnumerate) {
    g_oss.str("");

    common::VariadicCallEnumerate<16>(
        [](size_t index) { g_oss << index << " "; });

    common::VariadicCallEnumerate<4, 8>(
        [](auto index) { g_oss << index << " "; });

    ASSERT_EQ("0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 4 5 6 7 ", g_oss.str());
}

/******************************************************************************/
// VariadicMapIndex

class Functor
{
public:
    Functor() { }

    template <typename Type>
    void Run(const Type& t) {
        (void)t;
    }
};

TEST(Meta, VariadicMapIndex) {
    g_oss.str("");

    auto res = common::VariadicMapIndex(
        [](auto index, auto a) {
            Functor().Run(a);
            return a + decltype(a)(index);
        },
        // some argument to map.
        static_cast<int>(42), static_cast<double>(5), 'h');

    ASSERT_EQ(42, std::get<0>(res));
    ASSERT_EQ(6, std::get<1>(res));
    ASSERT_EQ('j', std::get<2>(res));
}

/******************************************************************************/
// VariadicMapIndex

TEST(Meta, VariadicMapEnumerate) {
    g_oss.str("");

    auto res = common::VariadicMapEnumerate<3>(
        [](auto index) {
            (void)index;
            return std::get<decltype(index)::index>(my_tuple);
        });

    // yay, the above is just a totally complicated identity map.
    ASSERT_EQ(my_tuple, res);
}

/******************************************************************************/
