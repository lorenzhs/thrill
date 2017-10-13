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

static const std::vector<std::string> known_configs = {
    "4x16_CRC32_m7",
    "4x8_CRC32_m7",
    "4x8_CRC32_m5",
    "4x8_CRC32_m3",
    "4x4_CRC32_m5",
    "4x4_CRC32_m3",
    "4x2_CRC32_m4",
    "1x4_CRC32",
    "1x2_CRC32",
    "4x16_Tab_m7",
    "4x8_Tab_m7",
    "4x8_Tab_m5",
    "4x8_Tab_m3",
    "4x4_Tab_m5",
    "4x4_Tab_m3",
    "4x2_Tab_m4",
    "1x4_Tab",
    "1x2_Tab"
};

template <typename Functor, typename Manipulator>
void run_accuracy(Functor &&test, const Manipulator &manip,
                  const std::string& name) {

    test(CRC32Config<16, 4, 7>{}, "4x16_CRC32_m7", manip, name);
    test(CRC32Config<8, 4, 7>{}, "4x8_CRC32_m7", manip, name);
    test(CRC32Config<8, 4, 5>{}, "4x8_CRC32_m5", manip, name);
    test(CRC32Config<8, 4, 3>{}, "4x8_CRC32_m3", manip, name);
    test(CRC32Config<4, 4, 5>{}, "4x4_CRC32_m5", manip, name);
    test(CRC32Config<4, 4, 3>{}, "4x4_CRC32_m3", manip, name);
    test(CRC32Config<2, 4, 4>{}, "4x2_CRC32_m4", manip, name);
    test(CRC32Config<4, 1>{}, "1x4_CRC32", manip, name);
    test(CRC32Config<2, 1>{}, "1x2_CRC32", manip, name);

    test(TabConfig<16, 4, 7>{}, "4x16_Tab_m7", manip, name);
    test(TabConfig<8, 4, 7>{}, "4x8_Tab_m7", manip, name);
    test(TabConfig<8, 4, 5>{}, "4x8_Tab_m5", manip, name);
    test(TabConfig<8, 4, 3>{}, "4x8_Tab_m3", manip, name);
    test(TabConfig<4, 4, 5>{}, "4x4_Tab_m5", manip, name);
    test(TabConfig<4, 4, 3>{}, "4x4_Tab_m3", manip, name);
    test(TabConfig<2, 4, 4>{}, "4x2_Tab_m4", manip, name);
    test(TabConfig<4, 1>{}, "1x4_Tab", manip, name);
    test(TabConfig<2, 1>{}, "1x2_Tab", manip, name);
}

#endif

/******************************************************************************/
