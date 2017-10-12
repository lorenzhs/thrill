/*******************************************************************************
 * examples/checkers/timings.hpp
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
#ifndef THRILL_EXAMPLES_CHECKERS_TIMINGS_HEADER
#define THRILL_EXAMPLES_CHECKERS_TIMINGS_HEADER

#include <string>
#include <vector>

static const std::vector<std::string> known_configs = {
    "16x16_Tab64_m15",
    "8x256_Tab64_m15",
    "5x128_Tab64_m11",
    "4x256_CRC32_m15",
    "8x16_CRC32_m15",
    "6x32_CRC32_m9",
    "5x16_CRC32_m5",
    "unchecked"
};

template <typename Functor>
void run_timings(Functor &&test) {
    test(Tab64Config<16, 16, 15>{}, "16x16_Tab64_m15");
    test(Tab64Config<256, 8, 15>{}, "8x256_Tab64_m15");
    test(Tab64Config<128, 5, 11>{}, "5x128_Tab64_m11");
    test(CRC32Config<256, 4, 15>{}, "4x256_CRC32_m15");
    test(CRC32Config<16, 8, 15>{}, "8x16_CRC32_m15");
    test(CRC32Config<32, 6, 9>{}, "6x32_CRC32_m9");
    test(CRC32Config<16, 5, 5>{}, "5x16_CRC32_m5");
#if 0
    test(CRC32Config<256, 2>{}, "2x256_CRC32");
    test(CRC32Config<256, 1>{}, "1x256_CRC32");

    test(CRC32Config<16, 4>{}, "4x16_CRC32");
    test(CRC32Config<16, 2>{}, "2x16_CRC32");
    test(CRC32Config<16, 1>{}, "1x16_CRC32");

    test(CRC32Config<4, 8>{}, "8x4_CRC32");
    test(CRC32Config<4, 6>{}, "6x4_CRC32");
    test(CRC32Config<4, 4>{}, "4x4_CRC32");
    test(CRC32Config<4, 3>{}, "3x4_CRC32");
    test(CRC32Config<4, 2>{}, "2x4_CRC32");
    test(CRC32Config<4, 1>{}, "1x4_CRC32");

    test(CRC32Config<2, 8>{}, "8x2_CRC32");
    test(CRC32Config<2, 6>{}, "6x2_CRC32");
    test(CRC32Config<2, 4>{}, "4x2_CRC32");
    test(CRC32Config<2, 3>{}, "3x2_CRC32");
    test(CRC32Config<2, 2>{}, "2x2_CRC32");
    test(CRC32Config<2, 1>{}, "1x2_CRC32");
#endif

    /*
    test(TabConfig<16, 1, 31>{}, "1x16_m31_Tab");
    test(TabConfig<16, 1, 15>{}, "1x16_m15_Tab");
    test(TabConfig<16, 1, 7>{}, "1x16_m7_Tab");
    test(TabConfig<16, 1, 3>{}, "1x16_m3_Tab");
    test(TabConfig<16, 1>{}, "1x16_Tab");
    test(TabConfig<4, 1>{}, "1x4_Tab");
    test(TabConfig<2, 1>{}, "1x2_Tab");
    */
#if 0
    test(TabConfig<256, 2>{}, "2x256_Tab");
    test(TabConfig<256, 1>{}, "1x256_Tab");

    test(TabConfig<16, 4>{}, "4x16_Tab");
    test(TabConfig<16, 2>{}, "2x16_Tab");
    test(TabConfig<16, 1>{}, "1x16_Tab");

    test(TabConfig<4, 8>{}, "8x4_Tab");
    test(TabConfig<4, 6>{}, "6x4_Tab");
    test(TabConfig<4, 4>{}, "4x4_Tab");
    test(TabConfig<4, 3>{}, "3x4_Tab");
    test(TabConfig<4, 2>{}, "2x4_Tab");
    test(TabConfig<4, 1>{}, "1x4_Tab");

    test(TabConfig<2, 8>{}, "8x2_Tab");
    test(TabConfig<2, 6>{}, "6x2_Tab");
    test(TabConfig<2, 4>{}, "4x2_Tab");
    test(TabConfig<2, 3>{}, "3x2_Tab");
    test(TabConfig<2, 2>{}, "2x2_Tab");
    test(TabConfig<2, 1>{}, "1x2_Tab");
#endif
}

#endif

/******************************************************************************/
