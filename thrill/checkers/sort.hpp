/*******************************************************************************
 * thrill/checkers/sort.hpp
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
#ifndef THRILL_CHECKERS_SORT_HEADER
#define THRILL_CHECKERS_SORT_HEADER

#include <thrill/api/context.hpp>
#include <thrill/checkers/functional.hpp>
#include <thrill/checkers/manipulator.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/define.hpp>

#include <vector>

namespace thrill {
namespace checkers {

class SortCheckerDummy : public noncopynonmove
{
public:
    SortCheckerDummy() = default;
    void reset() { }

    template <typename Ignored>
    void add_pre(const Ignored&) { }

    template <typename Ignored>
    void add_post(const Ignored&) { }

    template <typename Ignored>
    bool check(Ignored&) { return true; }
};

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
          typename Hash = common::HashCrc32<ValueType> >
class SortChecker : public noncopynonmove
{
    static const bool debug = false;

public:
    /*!
     * Construct a checker
     *
     * \param cmp_ Compare function to use
     */
    explicit SortChecker(CompareFunction cmp_ = CompareFunction { })
        : cmp(cmp_) { reset(); }

    //! Reset the checker's internal state
    void reset() {
        count_pre = 0;
        count_post = 0;
        sum_pre = 0;
        sum_post = 0;
        sorted_ = true;
    }

    //! Process an input element (before sorting)
    TLX_ATTRIBUTE_ALWAYS_INLINE
    void add_pre(const ValueType& v) {
        sum_pre += hash(v);
        ++count_pre;
    }

    /*!
     * Process an output element (after sorting)
     *
     * \param v Element to process
     */
    TLX_ATTRIBUTE_ALWAYS_INLINE
    void add_post(const ValueType& v) {
        if (TLX_LIKELY(count_post > 0) && cmp(v, last_post)) {
            sLOG << "Non-sorted values in output";  // << last_post << v;
            sorted_ = false;
        }
        last_post = v;

        // Init "first" (= minimum)
        if (TLX_UNLIKELY(count_post == 0)) { first_post = v; }

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
            sLOG << "check(): predecessor has larger item";
            sorted_ = false;
        }

        int unsorted_count = sorted_ ? 0 : 1;
        unsorted_count = ctx.net.AllReduce(unsorted_count);

        LOGC(debug && ctx.my_rank() == 0 && unsorted_count > 0)
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
        std::array<uint64_t, 4> sum { { count_pre, count_post, sum_pre, sum_post } };
        sum = ctx.net.AllReduce(sum, common::ComponentSum<decltype(sum)>());

        const bool success = (sum[0] == sum[1]) && (sum[2] == sum[3]);

        LOGC(debug && !success && ctx.my_rank() == 0)
            << common::log::fg_red() << common::log::bold()
            << "check() permutation: " << sum[0] << " pre-items, " << sum[1]
            << " post-items; check FAILED!!!!! Global pre-sum: " << sum[2]
            << " global post-sum: " << sum[3] << common::log::reset();

        LOGC(debug && success && ctx.my_rank() == 0)
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
        return /* is_sorted(ctx) && */ is_likely_permutation(ctx);
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
struct SortManipulatorDummy : public ManipulatorBase {
    template <typename Ignored>
    void operator () (Ignored) { }
    bool made_changes() const { return false; }
    void reset() { }
};

//! Drop last element from vector
struct SortManipulatorDropLast : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() > 1) { // don't leave it empty
            vec.pop_back();
            made_changes_ = true;
        }
    }
};

//! Add a default-constructed element to empty vectors
struct SortManipulatorAddToEmpty : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() == 0) {
            vec.emplace_back();
            made_changes_ = true;
        }
    }
};

//! Set second element equal to first
struct SortManipulatorSetEqual : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() >= 2 && vec[0] != vec[1]) {
            vec[1] = vec[0];
            made_changes_ = true;
        }
    }
};

//! Reset first element to default-constructed value
struct SortManipulatorResetToDefault : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() > 0 && vec[0] != ValueType()) {
            vec[0] = ValueType();
            made_changes_ = true;
        }
    }
};

//! Reset first element to default-constructed value
struct SortManipulatorIncFirst : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() > 0) {
            vec[0]++;
            made_changes_ = true;
        }
    }
};

//! Reset first element to default-constructed value
struct SortManipulatorRandFirst : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (vec.size() > 0) {
            ValueType old = vec[0];
            do {
                vec[0] = static_cast<ValueType>(rng());
            } while (vec[0] == old);
            made_changes_ = true;
        }
    }

private:
    std::mt19937 rng { std::random_device { } () };
};

//! Duplicate the last element of the first (local) block
struct SortManipulatorDuplicateLast : public ManipulatorBase {
    template <typename ValueType>
    void operator () (std::vector<ValueType>& vec) {
        if (!made_changes_ && vec.size() > 0) {
            vec.push_back(vec.back());
            made_changes_ = true;
        }
    }
};

//! Move the last element of the first (local) block to the beginning of the
//! second block, if one exists. Otherwise the element is dropped.
template <typename ValueType>
struct SortManipulatorMoveToNextBlock : public ManipulatorBase {
    void operator () (std::vector<ValueType>& vec) {
        if (!made_changes_ && vec.size() > 0) {
            tmp_ = vec.back();
            vec.pop_back();
            has_stored_ = true;
            made_changes_ = true;
        }
        else if (has_stored_) {
            vec.insert(vec.begin(), tmp_);
            has_stored_ = false;
        }
    }

protected:
    bool      has_stored_ = false;
    ValueType tmp_;
};

} // namespace checkers
} // namespace thrill

#endif // !THRILL_CHECKERS_SORT_HEADER

/******************************************************************************/
