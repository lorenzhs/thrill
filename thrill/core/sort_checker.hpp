/*******************************************************************************
 * thrill/core/sort_checker.hpp
 *
 * Probabilistic sort checker
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_SORT_CHECKER_HEADER
#define THRILL_CORE_SORT_CHECKER_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>

#include <vector>

namespace thrill {
namespace core {
namespace checkers {

/*!
 * Probabilistic checker for sorting algorithms
 *
 * \tparam ValueType Type of the elements being sorted
 *
 * \tparam CompareFunction Type of the compare function
 *
 * \tparam Hash Type of the hash function. Defaults to CRC32-C
 *
 * \ingroup api_layer
 */
template <typename ValueType,
          typename CompareFunction,
          typename Hash = common::hash_crc32<ValueType>>
class SortChecker
{
    static const bool debug = false;

public:
    /*!
     * Construct a checker
     *
     * \param cmp_ Compare function to use
     */
    explicit SortChecker(CompareFunction cmp_) : cmp(cmp_) { reset(); }

    //! Reset the checker's internal state
    void reset() {
        count_pre = 0;
        count_post = 0;
        sum_pre = 0;
        sum_post = 0;
        sorted_ = true;
    }

    //! Process an input element (before sorting)
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    void add_pre(const ValueType& v) {
        sum_pre += hash(v);
        ++count_pre;
    }

    /*!
     * Process an output element (after sorting)
     *
     * \param v Element to process
     */
    THRILL_ATTRIBUTE_ALWAYS_INLINE
    void add_post(const ValueType& v) {
        if (THRILL_LIKELY(count_post > 0) && cmp(v, last_post)) {
            sLOG1 << "Non-sorted values in output";  // << last_post << v;
            sorted_ = false;
        }
        last_post = v;

        // Init "first" (= minimum)
        if (THRILL_UNLIKELY(count_post == 0)) { first_post = v; }

        sum_post += hash(v);
        ++count_post;
    }

    /*!
     * Verify that the output elements seen at all workers were in globally
     * sorted order.
     *
     * \param ctx Thrill Context to use for communication
     */
    bool is_sorted(api::Context& ctx) {
        std::vector<ValueType> send;

        if (count_post > 0) { send.push_back(last_post); }
        auto recv = ctx.net.Predecessor(1, send);

        // If any predecessor PE has an item, and we have one,
        // check that the predecessor is smaller
        if (recv.size() > 0 && count_post > 0 && cmp(first_post, recv[0])) {
            sLOG1 << "check(): predecessor has larger item";
            sorted_ = false;
        }

        int unsorted_count = sorted_ ? 0 : 1;
        unsorted_count = ctx.net.AllReduce(unsorted_count);

        LOGC (ctx.my_rank() == 0 && unsorted_count > 0)
            << common::log::fg_red() << common::log::bold() << unsorted_count
            << " of " << ctx.num_workers()
            << " PEs have output that isn't sorted" << common::log::reset();

        return (unsorted_count == 0);
    }

    /*!
     * Verify probabilistically whether the output elements at all workers are a
     * permutation of the input elements.  Success probability depends on the
     * hash function used.
     *
     * This function has one-sided error -- it may wrongly accept an incorrect
     * output, but will never cry wolf on a correct one.
     *
     * \param ctx Thrill Context to use for communication
     */
    bool is_likely_permutation(api::Context& ctx) {
        std::array<uint64_t, 4> sum{{count_pre, count_post, sum_pre, sum_post}};
        sum = ctx.net.AllReduce(sum, common::ComponentSum<decltype(sum)>());

        const bool success = (sum[0] == sum[1]) && (sum[2] == sum[3]);

        LOGC (!success && ctx.my_rank() == 0)
            << common::log::fg_red() << common::log::bold()
            << "check() permutation: " << sum[0] << " pre-items, " << sum[1]
            << " post-items; check FAILED!!!!! Global pre-sum: " << sum[2]
            << " global post-sum: " << sum[3] << common::log::reset();

        LOGC (success&& debug&& ctx.my_rank() == 0)
            << "check() permutation: " << sum[0] << " pre-items, " << sum[1]
            << " post-items; check successful. Global pre-sum: " << sum[2]
            << " global post-sum: " << sum[3];

        return success;
    }

    /*!
     * Check correctness of the sorting procedure.  See `is_sorted` and
     * `is_likely_permutation` for more details.
     *
     * \param ctx Thrill Context to use for communication
     */
    bool check(api::Context& ctx) {
        return is_sorted(ctx) && is_likely_permutation(ctx);
    }

protected:
    //! Number of items seen in input and output
    uint64_t count_pre, count_post;
    //! Sum of hash values in input and output
    uint64_t sum_pre, sum_post;
    //! First and last element seen in output (used to verify global sortedness)
    ValueType first_post, last_post;
    //! Hash function
    Hash hash;
    //! Element comparison function
    CompareFunction cmp;
    //! Whether the local output was in sorted order
    bool sorted_;
};

//! Dummy no-op sort manipulator
struct SortManipulatorDummy {
    template <typename Ignored>
    void operator()(Ignored) {}
    bool made_changes() const { return false; }
};

//! Drop last element from vector
struct SortManipulatorDropLast {
    template <typename ValueType>
    void operator()(std::vector<ValueType>& vec) {
        if (vec.size() > 0) {
            vec.pop_back();
            made_changes_ = true;
        }
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Add a default-constructed element to empty vectors
struct SortManipulatorAddToEmpty {
    template <typename ValueType>
    void operator()(std::vector<ValueType>& vec) {
        if (vec.size() == 0) {
            vec.emplace_back();
            made_changes_ = true;
        }
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Set second element equal to first
struct SortManipulatorSetEqual {
    template <typename ValueType>
    void operator()(std::vector<ValueType>& vec) {
        if (vec.size() >= 2 && vec[0] != vec[1]) {
            vec[1] = vec[0];
            made_changes_ = true;
        }
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

//! Reset first element to default-constructed value
struct SortManipulatorResetToDefault {
    template <typename ValueType>
    void operator()(std::vector<ValueType>& vec) {
        if (vec.size() > 0 && vec[0] != ValueType()) {
            vec[0] = ValueType();
            made_changes_ = true;
        }
    }
    bool made_changes() const { return made_changes_; }

protected:
    bool made_changes_ = false;
};

}  // namespace checkers
}  // namespace core
}  // namespace thrill

#endif  // !THRILL_CORE_SORT_CHECKER_HEADER

/******************************************************************************/
