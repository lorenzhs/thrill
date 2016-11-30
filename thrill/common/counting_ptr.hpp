/*******************************************************************************
 * thrill/common/counting_ptr.hpp
 *
 * An intrusive reference counting pointer which is much more light-weight than
 * std::shared_ptr.
 *
 * Borrowed of STXXL under the Boost license. See http://stxxl.sourceforge.net
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2010-2011 Raoul Steffen <R-Steffen@gmx.de>
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_COUNTING_PTR_HEADER
#define THRILL_COMMON_COUNTING_PTR_HEADER

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <ostream>
#include <type_traits>

namespace thrill {
namespace common {

//! default deleter for CountingPtr
class DefaultCountingPtrDeleter
{
public:
    template <typename Type>
    void operator () (Type* ptr) const noexcept {
        delete ptr;
    }
};

/*!
 * High-performance smart pointer used as a wrapping reference counting pointer.
 *
 * This smart pointer class requires two functions in the template type: void
 * IncReference() and void DecReference(). These must increment and decrement a
 * reference count inside the templated object. When initialized, the type must
 * have reference count zero. Each new object referencing the data calls
 * IncReference() and each destroying holder calls del_reference(). When the
 * data object determines that it's internal count is zero, then it must destroy
 * itself.
 *
 * Accompanying the CountingPtr is a class ReferenceCount, from which reference
 * counted classes may be derive from. The class ReferenceCount implement all
 * methods required for reference counting.
 *
 * The whole method is more similar to boost's instrusive_ptr, but also yields
 * something resembling std::shared_ptr. However, compared to std::shared_ptr,
 * this class only contains a single pointer, while shared_ptr contains two
 * which are only related if constructed with std::make_shared.
 */
template <typename Type,
          typename Deleter = DefaultCountingPtrDeleter>
class CountingPtr
{
public:
    //! contained type.
    using element_type = Type;

private:
    //! the pointer to the currently referenced object.
    Type* ptr_;

    //! increment reference count for current object.
    void IncReference() noexcept
    { IncReference(ptr_); }

    //! increment reference count of other object.
    void IncReference(Type* o) noexcept
    { if (o) o->IncReference(); }

    //! decrement reference count of current object and maybe delete it.
    void DecReference() noexcept
    { if (ptr_ && ptr_->DecReference()) Deleter()(ptr_); }

public:
    //! all CountingPtr are friends such that they may steal pointers.
    template <typename Other, typename OtherDeleter>
    friend class CountingPtr;

    //! default constructor: contains a nullptr pointer.
    CountingPtr() noexcept
        : ptr_(nullptr) { }

    //! implicit construction from nullptr_t: contains a nullptr pointer.
    CountingPtr(std::nullptr_t) noexcept // NOLINT
        : ptr_(nullptr) { }

    //! constructor from pointer: initializes new reference to ptr.
    explicit CountingPtr(Type* ptr) noexcept
        : ptr_(ptr) { IncReference(); }

    //! copy-constructor: also initializes new reference to ptr.
    CountingPtr(const CountingPtr& other) noexcept
        : ptr_(other.ptr_) { IncReference(); }

    //! copy-constructor: also initializes new reference to ptr.
    template <typename Down,
              typename = typename std::enable_if<
                  std::is_convertible<Down*, Type*>::value, void>::type>
    CountingPtr(const CountingPtr<Down, Deleter>& other) noexcept
        : ptr_(other.ptr_) { IncReference(); }

    //! move-constructor: just moves pointer, does not change reference counts.
    CountingPtr(CountingPtr&& other) noexcept
        : ptr_(other.ptr_) { other.ptr_ = nullptr; }

    //! move-constructor: just moves pointer, does not change reference counts.
    template <typename Down,
              typename = typename std::enable_if<
                  std::is_convertible<Down*, Type*>::value, void>::type>
    CountingPtr(CountingPtr<Down, Deleter>&& other) noexcept
        : ptr_(other.ptr_) { other.ptr_ = nullptr; }

    //! copy-assignment operator: acquire reference on new one and dereference
    //! current object.
    CountingPtr& operator = (const CountingPtr& other) noexcept {
        if (&other == this) return *this;
        IncReference(other.ptr_);
        DecReference();
        ptr_ = other.ptr_;
        return *this;
    }

    //! copy-assignment operator: acquire reference on new one and dereference
    //! current object.
    template <typename Down,
              typename = typename std::enable_if<
                  std::is_convertible<Down*, Type*>::value, void>::type>
    CountingPtr& operator = (
        const CountingPtr<Down, Deleter>& other) noexcept {
        if (&other == this) return *this;
        IncReference(other.ptr_);
        DecReference();
        ptr_ = other.ptr_;
        return *this;
    }

    //! move-assignment operator: move reference of other to current object.
    CountingPtr& operator = (CountingPtr&& other) noexcept {
        if (&other == this) return *this;
        DecReference();
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
        return *this;
    }

    //! move-assignment operator: move reference of other to current object.
    template <typename Down,
              typename = typename std::enable_if<
                  std::is_convertible<Down*, Type*>::value, void>::type>
    CountingPtr& operator = (CountingPtr<Down, Deleter>&& other) noexcept {
        if (&other == this) return *this;
        DecReference();
        ptr_ = other.ptr_;
        other.ptr_ = nullptr;
        return *this;
    }

    //! destructor: decrements reference count in ptr.
    ~CountingPtr()
    { DecReference(); }

    //! return the enclosed object as reference.
    Type& operator * () const noexcept {
        assert(ptr_);
        return *ptr_;
    }

    //! return the enclosed pointer.
    Type* operator -> () const noexcept {
        assert(ptr_);
        return ptr_;
    }

    //! return the enclosed pointer.
    Type * get() const noexcept
    { return ptr_; }

    //! test equality of only the pointer values.
    bool operator == (const CountingPtr& other) const noexcept
    { return ptr_ == other.ptr_; }

    //! test inequality of only the pointer values.
    bool operator != (const CountingPtr& other) const noexcept
    { return ptr_ != other.ptr_; }

    //! test equality of only the address pointed to
    bool operator == (Type* other) const noexcept
    { return ptr_ == other; }

    //! test inequality of only the address pointed to
    bool operator != (Type* other) const noexcept
    { return ptr_ != other; }

    //! cast to bool check for a nullptr pointer
    operator bool () const noexcept
    { return valid(); }

    //! test for a non-nullptr pointer
    bool valid() const noexcept
    { return (ptr_ != nullptr); }

    //! test for a nullptr pointer
    bool empty() const noexcept
    { return (ptr_ == nullptr); }

    //! if the object is referred by this CountingPtr only
    bool unique() const noexcept
    { return ptr_ && ptr_->unique(); }

    //! make and refer a copy if the original object was shared.
    void unify() {
        if (ptr_ && !ptr_->unique())
            operator = (new Type(*ptr_));
    }

    //! release contained pointer
    void reset() {
        DecReference();
        ptr_ = nullptr;
    }

    //! swap enclosed object with another counting pointer (no reference counts
    //! need change)
    void swap(CountingPtr& b) noexcept
    { std::swap(ptr_, b.ptr_); }
};

template <typename Type, typename ... Args>
CountingPtr<Type> MakeCounting(Args&& ... args) {
    return CountingPtr<Type>(new Type(std::forward<Args>(args) ...));
}

//! swap enclosed object with another counting pointer (no reference counts need
//! change)
template <typename A>
void swap(CountingPtr<A>& a1, CountingPtr<A>& a2) noexcept {
    a1.swap(a2);
}

//! print pointer
template <typename A>
std::ostream& operator << (std::ostream& os, const CountingPtr<A>& c) {
    return os << c.get();
}

/*!
 * Provides reference counting abilities for use with CountingPtr.
 *
 * Use as superclass of the actual object, this adds a reference_count
 * value. Then either use CountingPtr as pointer to manage references and
 * deletion, or just do normal new and delete.
 */
class ReferenceCount
{
private:
    //! the reference count is kept mutable for CountingPtr<const Type> to
    //! change the reference count.
    mutable std::atomic<size_t> reference_count_;

public:
    //! new objects have zero reference count
    ReferenceCount() noexcept
        : reference_count_(0) { }

    //! coping still creates a new object with zero reference count
    ReferenceCount(const ReferenceCount&) noexcept
        : reference_count_(0) { }

    //! assignment operator, leaves pointers unchanged
    ReferenceCount& operator = (const ReferenceCount&) noexcept
    { return *this; } // changing the contents leaves pointers unchanged

    ~ReferenceCount()
    { assert(reference_count_ == 0); }

public:
    //! Call whenever setting a pointer to the object
    void IncReference() const noexcept
    { ++reference_count_; }

    /*!
     * Call whenever resetting (i.e. overwriting) a pointer to the object.
     * IMPORTANT: In case of self-assignment, call AFTER IncReference().
     *
     * \return if the object has to be deleted (i.e. if it's reference count
     * dropped to zero)
     */
    bool DecReference() const noexcept
    { assert(reference_count_ > 0); return (--reference_count_ == 0); }

    //! Test if the ReferenceCount is referenced by only one CountingPtr.
    bool unique() const noexcept
    { return (reference_count_ == 1); }

    //! Return the number of references to this object (for debugging)
    size_t reference_count() const noexcept
    { return reference_count_; }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_COUNTING_PTR_HEADER

/******************************************************************************/
