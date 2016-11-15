/*******************************************************************************
 * thrill/api/size.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SIZE_HEADER
#define THRILL_API_SIZE_HEADER

#include <thrill/api/action_node.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/net/group.hpp>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class SizeNode final : public ActionResultNode<size_t>
{
    static constexpr bool debug = false;

    using Super = ActionResultNode<size_t>;
    using Super::context_;

public:
    template <typename ParentDIA>
    explicit SizeNode(const ParentDIA& parent)
        : Super(parent.ctx(), "Size", { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty) {

        if (parent_stack_empty_ &&
            dynamic_cast<CacheNode<ValueType>*>(parent.node().get()) != nullptr) {
            // Add as child, but do not receive items via PreOp Hook.
            LOG << "SizeNode: skipping callback, accessing CacheNode directly";
            parent.node()->AddChild(this);
        }
        else {
            // Hook PreOp(s)
            auto pre_op_fn = [this](const ValueType&) { ++local_size_; };

            auto lop_chain = parent.stack().push(pre_op_fn).fold();
            parent.node()->AddChild(this, lop_chain);
        }
    }

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) return false;
        local_size_ = file.num_items();
        return true;
    }

    //! Executes the size operation.
    void Execute() final {

        // check if parent is CacheNode -> read number of items directly
        CacheNode<ValueType>* parent_cache =
            dynamic_cast<CacheNode<ValueType>*>(parents_[0].get());

        if (parent_stack_empty_ && parent_cache != nullptr)
            local_size_ = parent_cache->NumItems();

        // get the number of elements that are stored on this worker
        LOG << "MainOp processing, sum: " << local_size_;

        // process the reduce, default argument is SumOp.
        global_size_ = context_.net.AllReduce(local_size_);
    }

    //! Returns result of global size.
    const size_t& result() const final {
        return global_size_;
    }

private:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;
    // Local size to be used.
    size_t local_size_ = 0;
    // Global size resulting from all reduce.
    size_t global_size_ = 0;
};

template <typename ValueType, typename Stack>
size_t DIA<ValueType, Stack>::Size() const {
    assert(IsValid());

    using SizeNode = api::SizeNode<ValueType>;
    auto node = common::MakeCounting<SizeNode>(*this);
    node->RunScope();
    return node->result();
}

template <typename ValueType, typename Stack>
Future<size_t> DIA<ValueType, Stack>::SizeFuture() const {
    assert(IsValid());

    using SizeNode = api::SizeNode<ValueType>;
    auto node = common::MakeCounting<SizeNode>(*this);
    return Future<size_t>(node);
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SIZE_HEADER

/******************************************************************************/
