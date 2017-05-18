/*******************************************************************************
 * thrill/io/typed_block.hpp
 *
 * Constructs a TypedBlock object containing as many elements elements plus
 * some metadata as fits into the given block size.
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_TYPED_BLOCK_HEADER
#define THRILL_IO_TYPED_BLOCK_HEADER

#include <thrill/common/config.hpp>
#include <thrill/io/bid.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/request.hpp>
#include <thrill/mem/aligned_allocator.hpp>

#include <array>

namespace thrill {
namespace io {

//! \addtogroup io_layer
//! \{

//! Block Manager Internals \internal
namespace mng_local {

//! \defgroup io_layer_internals Internals
//! \ingroup io_layer
//! Internals and support classes
//! \{

template <size_t Bytes>
class FillerStruct
{
    using Byte = unsigned char;
    Byte filler_array[Bytes];

public:
    FillerStruct() {
        LOG0 << "[" << static_cast<void*>(this) << "] filler_struct is constructed";
    }
};

template <>
class FillerStruct<0>
{
    using byte_type = unsigned char;

public:
    FillerStruct() {
        LOG0 << "[" << static_cast<void*>(this) << "] filler_struct<> is constructed";
    }
};

//! Contains data elements for \c TypedBlock , not intended for direct use.
template <typename Type, size_t Size>
class ElementBlock
{
public:
    using type = Type;
    using value_type = Type;
    using reference = Type &;
    using const_reference = const Type &;
    using pointer = type *;
    using iterator = pointer;
    using const_iterator = const type *;

    enum {
        size = Size //!< number of elements in the block
    };

    //! Array of elements of type Type
    std::array<value_type, size> elem_;

    ElementBlock() {
        LOG0 << "[" << static_cast<void*>(this) << "] element_block is constructed";
    }

    //! An operator to access elements in the block
    reference operator [] (size_t i) {
        return elem_[i];
    }

    //! Returns \c iterator pointing to the first element.
    iterator begin() {
        return elem_.data();
    }

    //! Returns \c const_iterator pointing to the first element.
    const_iterator begin() const {
        return elem_.data();
    }

    //! Returns \c const_iterator pointing to the first element.
    const_iterator cbegin() const {
        return begin();
    }

    //! Returns \c iterator pointing to the end element.
    iterator end() {
        return elem_.data() + size;
    }

    //! Returns \c const_iterator pointing to the end element.
    const_iterator end() const {
        return elem_.data() + size;
    }

    //! Returns \c const_iterator pointing to the end element.
    const_iterator cend() const {
        return end();
    }
};

//! Contains BID references for \c TypedBlock , not intended for direct use.
template <typename Type, size_t Size, size_t RawSize, size_t NBids = 0>
class BlockWithBids : public ElementBlock<Type, Size>
{
public:
    enum {
        raw_size = RawSize,
        nbids = NBids
    };

    using BidType = BID<raw_size>;

    //! Array of BID references
    std::array<BidType, nbids> ref_;

    //! An operator to access bid references
    BidType& operator () (size_t i) {
        return ref_[i];
    }

    BlockWithBids() {
        LOG0 << "[" << static_cast<void*>(this) << "] block_w_bids is constructed";
    }
};

template <typename Type, size_t Size, size_t RawSize>
class BlockWithBids<Type, Size, RawSize, 0>
    : public ElementBlock<Type, Size>
{
public:
    enum {
        raw_size = RawSize,
        nbids = 0
    };

    using bid_type = BID<raw_size>;

    BlockWithBids() {
        LOG0 << "[" << static_cast<void*>(this) << "] block_w_bids<> is constructed";
    }
};

//! Contains per block information for \c TypedBlock , not intended for direct use.
template <typename Type, size_t RawSize, size_t NBids, typename MetaInfoType = void>
class BlockWithInfo
    : public BlockWithBids<Type, ((RawSize - sizeof(BID<RawSize>)* NBids - sizeof(MetaInfoType)) / sizeof(Type)), RawSize, NBids>
{
public:
    //! Type of per block information element.
    using info_type = MetaInfoType;

    //! Per block information element.
    info_type info;

    BlockWithInfo() {
        LOG0 << "[" << static_cast<void*>(this) << "] block_w_info is constructed";
    }
};

template <typename Type, size_t RawSize, size_t NBids>
class BlockWithInfo<Type, RawSize, NBids, void>
    : public BlockWithBids<Type, ((RawSize - sizeof(BID<RawSize>)* NBids) / sizeof(Type)), RawSize, NBids>
{
public:
    using info_type = void;

    BlockWithInfo() {
        LOG0 << "[" << static_cast<void*>(this) << "] block_w_info<> is constructed";
    }
};

//! Contains per block filler for \c TypedBlock , not intended for direct use.
template <typename BaseType, size_t FillSize = 0>
class AddFiller : public BaseType
{
private:
    //! Per block filler element.
    FillerStruct<FillSize> filler;

public:
    AddFiller() {
        LOG0 << "[" << static_cast<void*>(this) << "] add_filler is constructed";
    }
};

template <typename BaseType>
class AddFiller<BaseType, 0>
    : public BaseType
{
public:
    AddFiller() {
        LOG0 << "[" << static_cast<void*>(this) << "] add_filler<> is constructed";
    }
};

//! Helper to compute the size of the filler , not intended for direct use.
template <typename Type, size_t RawSize>
class ExpandStruct : public AddFiller<Type, RawSize - sizeof(Type)>
{ };

//! \}

} // namespace mng_local

//! Block containing elements of fixed length.
//!
//! \tparam RawSize size of block in bytes
//! \tparam Type type of block's records
//! \tparam NRef number of block references (BIDs) that can be stored in the block (default is 0)
//! \tparam MetaInfoType type of per block information (default is no information - void)
//!
//! The data array of type Type is contained in the parent class \c ElementBlock, see related information there.
//! The BID array of references is contained in the parent class \c BlockWithBids, see related information there.
//! The "per block information" is contained in the parent class \c BlockWithInfo, see related information there.
//!  \warning If \c RawSize > 2MB object(s) of this type can not be allocated on the stack (as a
//! function variable for example), because Linux POSIX library limits the stack size for the
//! main thread to (2MB - system page size)
template <size_t RawSize, typename Type, size_t NRef = 0, typename MetaInfoType = void>
class TypedBlock
    : public mng_local::ExpandStruct<
          mng_local::BlockWithInfo<Type, RawSize, NRef, MetaInfoType>, RawSize>
{
    using Base = mng_local::ExpandStruct<
              mng_local::BlockWithInfo<Type, RawSize, NRef, MetaInfoType>, RawSize>;

    static constexpr bool debug = false;

public:
    using value_type = Type;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using iterator = pointer;
    using const_pointer = const value_type *;
    using const_iterator = const_pointer;

    enum {
        raw_size = RawSize,                                        //!< size of block in bytes
        size = Base::size,                                         //!< number of elements in block
        has_only_data = (raw_size == (size * sizeof(value_type)))  //!< no meta info, bids or (non-empty) fillers included in the block, allows value_type array addressing across block boundaries
    };

    using bid_type = BID<raw_size>;

    TypedBlock() {
        static_assert(sizeof(TypedBlock) == raw_size, "Incorrect block size!");
        LOG << "[" << static_cast<void*>(this) << "] TypedBlock is constructed";
#if 0
        assert(((long)this) % THRILL_DEFAULT_ALIGN == 0);
#endif
    }

#if 0
    TypedBlock(const TypedBlock& tb) {
        THRILL_STATIC_ASSERT(sizeof(TypedBlock) == raw_size);
        LOG << "[" << static_cast<void*>(this)
            << "] TypedBlock is copy constructed from "
            << "[" << static_cast<void*>(&tb) << "]";
        tlx::UNUSED(tb);
    }
#endif

    /*!
     *  Writes block to the disk(s).
     * \param bid block identifier, points the file(disk) and position
     * \param on_cmpl completion handler
     * \return \c pointer_ptr object to track status I/O operation after the call
     */
    RequestPtr write(const bid_type& bid,
                     CompletionHandler on_cmpl = CompletionHandler()) {
        LOG << "BLC:write  " << bid;
        return bid.storage->awrite(this, bid.offset, raw_size, on_cmpl);
    }

    /*!
     * Reads block from the disk(s).
     * \param bid block identifier, points the file(disk) and position
     * \param on_cmpl completion handler
     * \return \c pointer_ptr object to track status I/O operation after the call
     */
    RequestPtr read(const bid_type& bid,
                    CompletionHandler on_cmpl = CompletionHandler()) {
        LOG << "BLC:read   " << bid;
        return bid.storage->aread(this, bid.offset, raw_size, on_cmpl);
    }

    /*!
     *  Writes block to the disk(s).
     * \param bid block identifier, points the file(disk) and position
     * \param on_cmpl completion handler
     * \return \c pointer_ptr object to track status I/O operation after the call
     */
    RequestPtr write(const BID<0>& bid,
                     CompletionHandler on_cmpl = CompletionHandler()) {
        LOG << "BLC:write  " << bid;
        assert(bid.size >= raw_size);
        return bid.storage->awrite(this, bid.offset, raw_size, on_cmpl);
    }

    /*!
     * Reads block from the disk(s).
     * \param bid block identifier, points the file(disk) and position
     * \param on_cmpl completion handler
     * \return \c pointer_ptr object to track status I/O operation after the call
     */
    RequestPtr read(const BID<0>& bid,
                    CompletionHandler on_cmpl = CompletionHandler()) {
        LOG << "BLC:read   " << bid;
        assert(bid.size >= raw_size);
        return bid.storage->aread(this, bid.offset, raw_size, on_cmpl);
    }

#if 0
    // STRANGE: implementing destructor makes g++ allocate
    // additional 4 bytes in the beginning of every array
    // of this type !? makes aligning to 4K boundaries difficult
    //
    // http://www.cc.gatech.edu/grads/j/Seung.Won.Jun/tips/pl/node4.html :
    // "One interesting thing is the array allocator requires more memory
    //  than the array size multiplied by the size of an element, by a
    //  difference of delta for metadata a compiler needs. It happens to
    //  be 8 bytes long in g++."
    ~TypedBlock() {
        LOG << "[" << static_cast<void*>(this) << "] TypedBlock is destructed";
    }
#endif
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_TYPED_BLOCK_HEADER

/******************************************************************************/
