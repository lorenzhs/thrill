/*******************************************************************************
 * thrill/io/request_queue_impl_qw_qr.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2005 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/request_queue_impl_qw_qr.hpp>
#include <thrill/io/serving_request.hpp>

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
 #include <windows.h>
#endif

#include <algorithm>

#ifndef THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
#define THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION 1
#endif

namespace thrill {
namespace io {

RequestQueueImplQwQr::RequestQueueImplQwQr(int n)
    : thread_state_(NOT_RUNNING) {
    common::UNUSED(n);
    StartThread(worker, static_cast<void*>(this), thread_, thread_state_);
}

void RequestQueueImplQwQr::AddRequest(RequestPtr& req) {
    if (req.empty())
        THRILL_THROW_INVALID_ARGUMENT("Empty request submitted to disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request submitted to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req.get()))
        LOG1 << "Incompatible request submitted to running queue.";

    if (req.get()->type() == Request::READ)
    {
#if THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> lock(write_mutex_);
            if (std::find_if(write_queue_.begin(), write_queue_.end(),
                             bind2nd(FileOffsetMatch(), req))
                != write_queue_.end())
            {
                LOG1 << "READ request submitted for a BID with a pending WRITE request";
            }
        }
#endif
        std::unique_lock<std::mutex> lock(read_mutex_);
        read_queue_.push_back(req);
    }
    else
    {
#if THRILL_CHECK_FOR_PENDING_REQUESTS_ON_SUBMISSION
        {
            std::unique_lock<std::mutex> lock(read_mutex_);
            if (std::find_if(read_queue_.begin(), read_queue_.end(),
                             bind2nd(FileOffsetMatch(), req))
                != read_queue_.end())
            {
                LOG1 << "WRITE request submitted for a BID with a pending READ request";
            }
        }
#endif
        std::unique_lock<std::mutex> lock(write_mutex_);
        write_queue_.push_back(req);
    }

    sem_.signal();
}

bool RequestQueueImplQwQr::CancelRequest(Request* req) {
    if (!req)
        THRILL_THROW_INVALID_ARGUMENT("Empty request canceled disk_queue.");
    if (thread_state_() != RUNNING)
        THRILL_THROW_INVALID_ARGUMENT("Request canceled to not running queue.");
    if (!dynamic_cast<ServingRequest*>(req))
        LOG1 << "Incompatible request submitted to running queue.";

    bool was_still_in_queue = false;
    if (req->type() == Request::READ)
    {
        std::unique_lock<std::mutex> lock(read_mutex_);
        Queue::iterator pos
            = std::find(read_queue_.begin(), read_queue_.end(), req);
        if (pos != read_queue_.end())
        {
            read_queue_.erase(pos);
            was_still_in_queue = true;
            lock.unlock();
            sem_.wait();
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(write_mutex_);
        Queue::iterator pos
            = std::find(write_queue_.begin(), write_queue_.end(), req);
        if (pos != write_queue_.end())
        {
            write_queue_.erase(pos);
            was_still_in_queue = true;
            lock.unlock();
            sem_.wait();
        }
    }

    return was_still_in_queue;
}

RequestQueueImplQwQr::~RequestQueueImplQwQr() {
    StopThread(thread_, thread_state_, sem_);
}

void* RequestQueueImplQwQr::worker(void* arg) {
    RequestQueueImplQwQr* pthis = static_cast<RequestQueueImplQwQr*>(arg);
    // pin I/O thread to last core
    common::SetCpuAffinity(std::thread::hardware_concurrency() - 1);

    bool write_phase = true;
    for ( ; ; )
    {
        pthis->sem_.wait();

        if (write_phase)
        {
            std::unique_lock<std::mutex> write_lock(pthis->write_mutex_);
            if (!pthis->write_queue_.empty())
            {
                RequestPtr req = pthis->write_queue_.front();
                pthis->write_queue_.pop_front();

                write_lock.unlock();

                // assert(req->get_reference_count()) > 1);
                dynamic_cast<ServingRequest*>(req.get())->serve();
            }
            else
            {
                write_lock.unlock();

                pthis->sem_.signal();

                if (pthis->priority_op_ == WRITE)
                    write_phase = false;
            }

            if (pthis->priority_op_ == NONE || pthis->priority_op_ == READ)
                write_phase = false;
        }
        else
        {
            std::unique_lock<std::mutex> read_lock(pthis->read_mutex_);

            if (!pthis->read_queue_.empty())
            {
                RequestPtr req = pthis->read_queue_.front();
                pthis->read_queue_.pop_front();

                read_lock.unlock();

                LOG << "queue: before serve request has " << req->reference_count() << " references ";
                // assert(req->get_reference_count() > 1);
                dynamic_cast<ServingRequest*>(req.get())->serve();
                LOG << "queue: after serve request has " << req->reference_count() << " references ";
            }
            else
            {
                read_lock.unlock();

                pthis->sem_.signal();

                if (pthis->priority_op_ == READ)
                    write_phase = true;
            }

            if (pthis->priority_op_ == NONE || pthis->priority_op_ == WRITE)
                write_phase = true;
        }

        // terminate if it has been requested and queues are empty
        if (pthis->thread_state_() == TERMINATING) {
            if (pthis->sem_.wait() == 0)
                break;
            else
                pthis->sem_.signal();
        }
    }

    pthis->thread_state_.set_to(TERMINATED);

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
    // Workaround for deadlock bug in Visual C++ Runtime 2012 and 2013, see
    // request_queue_impl_worker.cpp. -tb
    ExitThread(nullptr);
#else
    return nullptr;
#endif
}

} // namespace io
} // namespace thrill

/******************************************************************************/
