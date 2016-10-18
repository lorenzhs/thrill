/*******************************************************************************
 * thrill/api/reduce_to_index.hpp
 *
 * DIANode for a reduce operation. Performs the actual reduce operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_TO_INDEX_HEADER
#define THRILL_API_REDUCE_TO_INDEX_HEADER

#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/meta.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/core/reduce_by_index_post_phase.hpp>
#include <thrill/core/reduce_pre_phase.hpp>

#include <functional>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

class DefaultReduceToIndexConfig : public core::DefaultReduceConfig
{ };

/*!
 * A DIANode which performs a ReduceToIndex operation. ReduceToIndex groups the
 * elements in a DIA by their key and reduces every key bucket to a single
 * element each. The ReduceToIndexNode stores the key_extractor and the
 * reduce_function UDFs. The chainable LOps ahead of the Reduce operation are
 * stored in the Stack. The ReduceToIndexNode has the type ValueType, which is
 * the result type of the reduce_function. The key type is an unsigned integer
 * and the output DIA will have element with key K at index K.
 *
 * \tparam ParentType Input type of the Reduce operation
 * \tparam ValueType Output type of the Reduce operation
 * \tparam ParentStack Function stack, which contains the chained lambdas between the last and this DIANode.
 * \tparam KeyExtractor Type of the key_extractor function.
 * \tparam ReduceFunction Type of the reduce_function
 *
 * \ingroup api_layer
 */
template <typename ValueType,
          typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig, typename CheckingDriver,
          bool VolatileKey, bool SendPair>
class ReduceToIndexNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;
    using KeyValuePair = std::pair<Key, Value>;

    static_assert(std::is_same<Key, size_t>::value,
                  "Key must be an unsigned integer");

    using PrePhaseOutput =
              typename common::If<VolatileKey, KeyValuePair, Value>::type;

    using Checker = typename CheckingDriver::checker_t;
    using Manipulator = typename CheckingDriver::manipulator_t;

    static constexpr bool use_mix_stream_ = ReduceConfig::use_mix_stream_;
    static constexpr bool use_post_thread_ = ReduceConfig::use_post_thread_;

private:
    //! Emitter for PostPhase to push elements to next DIA object.
    class Emitter
    {
    public:
        explicit Emitter(ReduceToIndexNode* node, Checker& checker)
            : node_(node), checker_(checker) { }

        void operator () (const ValueType& item) const {
            checker_.add_post(item);
            return node_->PushItem(item);
        }

        void operator () (const ValueType& item, const KeyValuePair& pair) const {
            checker_.add_post(pair);
            return node_->PushItem(item);
        }

    private:
        ReduceToIndexNode* node_;
        Checker& checker_;
    };

    //! PreOp to insert elements into PrePhase
    template <typename V, typename PrePhase, typename Checker,
              typename KeyIsValue = void>
    struct ReducePreOp {
        ReducePreOp(PrePhase& pre_phase, const KeyExtractor&, Checker& checker)
            : pre_phase_(pre_phase), checker_(checker) { }
        auto operator () (const ValueType& input) {
            checker_.add_pre(input);
            return pre_phase_.Insert(input);
        }

    private:
        PrePhase& pre_phase_;
        Checker&  checker_;
    };

    //! PreOp to insert elements into PrePhase, handling the case where either
    //! Key or Value is equal to ValueType
    template <typename V, typename PrePhase, typename Checker>
    struct ReducePreOp<V, PrePhase, Checker,
                       typename std::enable_if_t<(std::is_same<std::decay_t<V>, Key>::value ||
                                                  std::is_same<std::decay_t<V>, Value>::value)> >{
        ReducePreOp(PrePhase& pre_phase, const KeyExtractor& key_extractor,
                    Checker& checker)
            : pre_phase_(pre_phase),
              key_extractor_(key_extractor),
              checker_(checker) { }
        auto operator () (const ValueType& input) {
            checker_.add_pre(key_extractor_(input), input);
            return pre_phase_.Insert(input);
        }

    private:
        PrePhase&           pre_phase_;
        const KeyExtractor& key_extractor_;
        Checker&            checker_;
    };

public:
    /*!
     * Constructor for a ReduceToIndexNode. Sets the parent, stack,
     * key_extractor and reduce_function.
     */
    template <typename ParentDIA>
    ReduceToIndexNode(const ParentDIA& parent,
                      const char* label,
                      const KeyExtractor& key_extractor,
                      const ReduceFunction& reduce_function,
                      size_t result_size,
                      const Value& neutral_element,
                      const ReduceConfig& config,
                      std::shared_ptr<CheckingDriver> driver)
        : Super(parent.ctx(), label, { parent.id() }, { parent.node() }),
          mix_stream_(use_mix_stream_ ?
                      parent.ctx().GetNewMixStream(this) : nullptr),
          cat_stream_(use_mix_stream_ ?
                      nullptr : parent.ctx().GetNewCatStream(this)),
          emitters_(use_mix_stream_ ?
                    mix_stream_->GetWriters() : cat_stream_->GetWriters()),
          result_size_(result_size),
          pre_phase_(
              context_, Super::id(), context_.num_workers(),
              key_extractor, reduce_function, emitters_, driver->manipulator(),
              config, core::ReduceByIndex<Key>(0, result_size)),
          post_phase_(
              context_, Super::id(),
              key_extractor, reduce_function, Emitter(this, driver->checker()),
              driver->manipulator(), config, core::ReduceByIndex<Key>(),
              neutral_element),
          checking_driver_(driver)
    {
        // Reset checker
        checking_driver_->reset();
        // Hook PreOp: Locally hash elements of the current DIA onto buckets and
        // reduce each bucket to a single value, afterwards send data to another
        // worker given by the shuffle algorithm.
        auto pre_op_fn = ReducePreOp<ValueType, decltype(pre_phase_), Checker>
                             (pre_phase_, key_extractor, driver->checker());
/*[this](const ValueType& input) {
                             checker_.add_pre(input);
                             return pre_phase_.Insert(input);
                             };*/

        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    DIAMemUse PreOpMemUse() final {
        // request maximum RAM limit, the value is calculated by StageBuilder,
        // and set as DIABase::mem_limit_.
        return DIAMemUse::Max();
    }

    void StartPreOp(size_t /* id */) final {
        if (!use_post_thread_) {
            // use pre_phase without extra thread
            pre_phase_.Initialize(DIABase::mem_limit_);

            // re-parameterize with resulting key range on this worker - this is
            // only know after Initialize() of the pre_phase_.
            post_phase_.table().index_function() =
                core::ReduceByIndex<Key>(
                    pre_phase_.key_range(context_.my_rank()));
        }
        else {
            pre_phase_.Initialize(DIABase::mem_limit_ / 2);

            // re-parameterize with resulting key range on this worker - this is
            // only know after Initialize() of the pre_phase_.
            post_phase_.table().index_function() =
                core::ReduceByIndex<Key>(
                    pre_phase_.key_range(context_.my_rank()));
            post_phase_.Initialize(DIABase::mem_limit_ / 2);

            // start additional thread to receive from the channel
            thread_ = common::CreateThread([this] { ProcessChannel(); });
        }
    }

    void StopPreOp(size_t /* id */) final {
        LOG << *this << " running StopPreOp";
        // Flush hash table before the postOp
        pre_phase_.FlushAll();
        pre_phase_.CloseAll();
        // waiting for the additional thread to finish the reduce
        if (use_post_thread_) thread_.join();
        use_mix_stream_ ? mix_stream_->Close() : cat_stream_->Close();
    }

    void Execute() final { }

    DIAMemUse PushDataMemUse() final {
        return DIAMemUse::Max();
    }

    void PushData(bool consume) final {

        if (!use_post_thread_ && !reduced_) {
            // not final reduced, and no additional thread, perform post reduce
            post_phase_.Initialize(DIABase::mem_limit_);
            ProcessChannel();

            reduced_ = true;
        }
        post_phase_.PushData(consume);
    }

    //! process the inbound data in the post reduce phase
    void ProcessChannel() {
        if (use_mix_stream_)
        {
            auto reader = mix_stream_->GetMixReader(/* consume */ true);
            sLOG << "reading data from" << mix_stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                post_phase_.Insert(reader.template Next<PrePhaseOutput>());
            }
        }
        else
        {
            auto reader = cat_stream_->GetCatReader(/* consume */ true);
            sLOG << "reading data from" << cat_stream_->id()
                 << "to push into post table which flushes to" << this->id();
            while (reader.HasNext()) {
                post_phase_.Insert(reader.template Next<PrePhaseOutput>());
            }
        }
    }

    void Dispose() final {
        post_phase_.Dispose();
    }

private:
    // pointers for both Mix and CatStream. only one is used, the other costs
    // only a null pointer.
    data::MixStreamPtr mix_stream_;
    data::CatStreamPtr cat_stream_;

    std::vector<data::Stream::Writer> emitters_;

    size_t result_size_;

    //! handle to additional thread for post phase
    std::thread thread_;

    core::ReducePrePhase<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, Manipulator,
        VolatileKey, ReduceConfig, core::ReduceByIndex<Key> > pre_phase_;

    core::ReduceByIndexPostPhase<
        ValueType, Key, Value, KeyExtractor, ReduceFunction, Emitter,
        Manipulator, SendPair, ReduceConfig> post_phase_;

    std::shared_ptr<CheckingDriver> checking_driver_;

    bool reduced_ = false;
};

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig, typename CheckingDriver>
auto DIA<ValueType, Stack>::ReduceToIndex(
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    const ValueType &neutral_element,
    const ReduceConfig &reduce_config,
    std::shared_ptr<CheckingDriver> driver) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::
                                template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<KeyExtractor>::result_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using ReduceNode = ReduceToIndexNode<
              DOpResult, KeyExtractor, ReduceFunction,
              ReduceConfig, CheckingDriver, /* VolatileKey */ false, false>;

    auto node = common::MakeCounting<ReduceNode>(
        *this, "ReduceToIndex", key_extractor, reduce_function,
        size, neutral_element, reduce_config, driver);

    return DIA<DOpResult>(node);
}

template <typename ValueType, typename Stack>
template <typename KeyExtractor, typename ReduceFunction,
          typename ReduceConfig, typename CheckingDriver>
auto DIA<ValueType, Stack>::ReduceToIndex(
    struct VolatileKeyTag const &,
    const KeyExtractor &key_extractor,
    const ReduceFunction &reduce_function,
    size_t size,
    const ValueType &neutral_element,
    const ReduceConfig &reduce_config,
    std::shared_ptr<CheckingDriver> driver) const {
    assert(IsValid());

    using DOpResult
              = typename common::FunctionTraits<ReduceFunction>::result_type;

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<0>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<ReduceFunction>::template arg<1>
            >::value,
        "ReduceFunction has the wrong input type");

    static_assert(
        std::is_same<
            DOpResult,
            ValueType>::value,
        "ReduceFunction has the wrong output type");

    static_assert(
        std::is_same<
            typename std::decay<typename common::FunctionTraits<KeyExtractor>::
                                template arg<0> >::type,
            ValueType>::value,
        "KeyExtractor has the wrong input type");

    static_assert(
        std::is_same<
            typename common::FunctionTraits<KeyExtractor>::result_type,
            size_t>::value,
        "The key has to be an unsigned long int (aka. size_t).");

    using ReduceNode = ReduceToIndexNode<
              DOpResult, KeyExtractor, ReduceFunction,
              ReduceConfig, CheckingDriver, /* VolatileKey */ true, false>;

    auto node = common::MakeCounting<ReduceNode>(
        *this, "ReduceToIndex", key_extractor, reduce_function,
        size, neutral_element, reduce_config, driver);

    return DIA<DOpResult>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_TO_INDEX_HEADER

/******************************************************************************/
