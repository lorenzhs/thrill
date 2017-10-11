/*******************************************************************************
 * examples/checkers/accuracy.hpp
 *
 * Not a real header, just factored out commons.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_CHECKERS_ACCURACY_HEADER
#define THRILL_EXAMPLES_CHECKERS_ACCURACY_HEADER

#include <tuple>

template <typename Functor, typename Manipulator, typename ... Args>
void run_accuracy(Context &ctx, Functor &&f, const Manipulator &manipulator,
                  const std::string& name, Args ... args) {

    auto arg_tuple = std::make_tuple(std::forward<Args>(args)...);

    auto test = [&f, &manipulator, &name, &arg_tuple, &ctx](
        auto config, const std::string& config_name) {
        auto arg = std::tuple_cat(
            std::tie(manipulator, config, name, config_name),
            arg_tuple);
        std::apply(f, arg)(ctx);
    };

    test(CRC32Config<16, 4, 7>{}, "4x16_CRC32_m7");
    test(CRC32Config<8, 4, 7>{}, "4x8_CRC32_m7");
    test(CRC32Config<8, 4, 5>{}, "4x8_CRC32_m5");
    test(CRC32Config<8, 4, 3>{}, "4x8_CRC32_m3");
    test(CRC32Config<4, 4, 5>{}, "4x4_CRC32_m5");
    test(CRC32Config<4, 4, 3>{}, "4x4_CRC32_m3");
    test(CRC32Config<2, 4, 4>{}, "4x2_CRC32_m4");
    test(CRC32Config<4, 1>{}, "1x4_CRC32");
    test(CRC32Config<2, 1>{}, "1x2_CRC32");

    test(TabConfig<16, 4, 7>{}, "4x16_Tab_m7");
    test(TabConfig<8, 4, 7>{}, "4x8_Tab_m7");
    test(TabConfig<8, 4, 5>{}, "4x8_Tab_m5");
    test(TabConfig<8, 4, 3>{}, "4x8_Tab_m3");
    test(TabConfig<4, 4, 5>{}, "4x4_Tab_m5");
    test(TabConfig<4, 4, 3>{}, "4x4_Tab_m3");
    test(TabConfig<2, 4, 4>{}, "4x2_Tab_m4");
    test(TabConfig<4, 1>{}, "1x4_Tab");
    test(TabConfig<2, 1>{}, "1x2_Tab");
}

#endif

/******************************************************************************/
