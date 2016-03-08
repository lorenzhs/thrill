/*******************************************************************************
 * examples/freq_items/freq_items_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/freq_items/freq_items.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>

using namespace thrill;           // NOLINT
using namespace examples::freqitems; // NOLINT

// hack to print std::pair
namespace std {
template <typename T1, typename T2>
ostream &operator << (ostream &os, const pair<T1, T2> &p) {
    return os << "(" << p.first << ", " << p.second << ")";
}
}

static auto RunFreqItems(api::Context& ctx, size_t num_elems, size_t num_items,
                         double eps, double delta) {
    // TODO: generate data where elements can actually occur more than once
    auto data = Generate(ctx, num_elems).Cache();
    auto result = FreqItems(data, num_items, eps, delta);

    LOG << "Result: " << result.first << " freq " << result.second;
    return result;
}

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;
    clp.SetVerboseProcess(false);

    size_t num_elems = 16*1024*1024, num_items = 10;
    double eps = 0.01, delta = 0.01;
    clp.AddSizeT('n', "num_elemes", num_elems, "Number of elements, default: 2^24");
    clp.AddSizeT('k', "num_items", num_items, "Items to select, default: 10");
    clp.AddDouble('e', "eps", eps, "Approximation quality, default: 0.01");
    clp.AddDouble('d', "delta", delta, "Failure probability, default: 0.01");

    if (!clp.Process(argc, argv)) {
        return -1;
    }
    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            RunFreqItems(ctx, num_elems, num_items, eps, delta);
        });
}

/******************************************************************************/
