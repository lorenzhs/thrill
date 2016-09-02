/*******************************************************************************
 * examples/word_count/word_count.hpp
 *
 * This file contains the WordCount core example. See word_count_run.cpp for how
 * to run it on different inputs.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER
#define THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER

#include <thrill/api/reduce_by_key.hpp>
#include <thrill/common/fast_string.hpp>
#include <thrill/common/string_view.hpp>

#include <string>
#include <utility>

namespace std {
template <typename S, typename T>
ostream &operator << (ostream &os, const pair<S,T> &p) {
    return os << "(" << p.first << ", " << p.second << ")";
}
}

namespace examples {
namespace word_count {

using namespace thrill; // NOLINT

using WordCountPair = std::pair<std::string, size_t>;

//! The most basic WordCount user program: reads a DIA containing std::string
//! words, and returns a DIA containing WordCountPairs.
template <typename InputStack>
auto WordCount(const DIA<std::string, InputStack>&input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
                /* map lambda: emit each word */
            common::SplitView(
                line, ' ', [&](const common::StringView& sv) {
                    if (sv.size() == 0) return;
                    emit(WordCountPair(sv.ToString(), 1));
                });
        });

    return word_pairs.ReduceByKey(
        common::TupleGet<0, WordCountPair>(),
        common::TupleReduceIndex<1, WordCountPair /*, std::plus<size_t> */>());
}

/******************************************************************************/

using FastWordCountPair = std::pair<common::FastString, size_t>;

//! An optimized WordCount user program: reads a DIA containing std::string
//! words, and returns a DIA containing WordCountPairs. In the reduce step our
//! FastString implementation is used to reduce the number of allocations.
template <typename InputStack>
auto FastWordCount(const DIA<std::string, InputStack>&input) {

    auto word_pairs = input.template FlatMap<FastWordCountPair>(
        [](const std::string& line, auto emit) -> void {
                /* map lambda: emit each word */
            common::SplitView(
                line, ' ', [&](const common::StringView& sv) {
                    if (sv.size() == 0) return;
                    emit(FastWordCountPair(sv.ToFastString(), 1));
                });
        });

    return word_pairs.ReducePair(std::plus<size_t>());
}

} // namespace word_count
} // namespace examples

#endif // !THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER

/******************************************************************************/
