/*******************************************************************************
 * examples/freq_items/freq_items.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_FREQITEMS_FREQITEMS_HEADER
#define THRILL_EXAMPLES_FREQITEMS_FREQITEMS_HEADER

#include <examples/select/select.hpp>

#include <thrill/api/collapse.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cmath>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

namespace examples {
namespace freqitems {

static constexpr bool debug = true;

static constexpr size_t base_case_size = 1024;

#define LOGM LOGC(debug && ctx.my_rank() == 0)

template <typename ValueType, typename InStack>
auto FreqItems(const DIA<ValueType, InStack> &data, size_t num,
               double eps, double delta) {
    using CountPair = std::pair<ValueType, size_t>;

    api::Context& ctx = data.context();
    const size_t size = data.Size();

    LOGM << "FreqItems with n = " << size << ", k = " << num
         << ", eps = " << eps << ", delta = " << delta;

    assert(0 <= num && num < size);
    CountPair result{};

    if (size < base_case_size) {
        // not worth it...  gather all data at worker with rank 0
        auto elements = data.Gather();

        if (ctx.my_rank() == 0) {
            assert(rank < elements.size());

            std::unordered_map<ValueType, size_t> counts;
            for (const ValueType &elem : elements) {
                counts[elem]++;
            }

            std::vector<CountPair> freqs;
            freqs.reserve(counts.size());
            for (const auto &pair : counts) {
                freqs.push_back(pair);
            }

            std::sort(freqs.begin(), freqs.end(),
                      [](const auto &a, const auto &b) {
                          return a.second > b.second || a.first > b.first;
                      });

            assert(num < freqs.size());

            result = freqs[num];
        }

        result = ctx.net.Broadcast(result);
        return result;
    }

    double p = std::min(1.0, 8.0 * log(static_cast<double>(2*num) / delta)/
                        (static_cast<double>(size) * pow(eps, 2)));
    LOGM << "Sampling rate p = " << p;

    auto sample =
        data.Sample(p).Map([](const ValueType &val) -> CountPair
                       {return std::make_pair(val, 1); })
        .ReducePair([](const size_t &a, const size_t &b) { return  a +b; });

    // does not terminate if I use LOGM instead of LOG
    LOG << "sample has size " << sample.Size();

    result = examples::select::Select(
        sample, num,
        [](const CountPair &a, const CountPair &b) -> bool {
            return a.second > b.second || a.first > b.first;
        });

    return result;
}

} // namespace freqitems
} // namespace examples

#endif // !THRILL_EXAMPLES_FREQITEMS_FREQITEMS_HEADER

/******************************************************************************/
