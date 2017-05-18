/*******************************************************************************
 * thrill/io/create_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2008, 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/config_file.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/linuxaio_file.hpp>
#include <thrill/io/memory_file.hpp>
#include <thrill/io/mmap_file.hpp>
#include <thrill/io/syscall_file.hpp>

#include <ostream>
#include <stdexcept>
#include <string>

namespace thrill {
namespace io {

FileBasePtr CreateFile(const std::string& io_impl,
                       const std::string& filename,
                       int options, int physical_device_id, int disk_allocator_id) {
    // construct temporary disk_config structure
    DiskConfig cfg(filename, 0, io_impl);
    cfg.queue = physical_device_id;
    cfg.direct =
        (options& FileBase::REQUIRE_DIRECT) ? DiskConfig::DIRECT_ON :
        (options& FileBase::DIRECT) ? DiskConfig::DIRECT_TRY :
        DiskConfig::DIRECT_OFF;

    return CreateFile(cfg, options, disk_allocator_id);
}

FileBasePtr CreateFile(DiskConfig& config, int mode, int disk_allocator_id) {
    // apply disk_config settings to open mode

    mode &= ~(FileBase::DIRECT | FileBase::REQUIRE_DIRECT); // clear DIRECT and REQUIRE_DIRECT

    switch (config.direct) {
    case DiskConfig::DIRECT_OFF:
        break;
    case DiskConfig::DIRECT_TRY:
        mode |= FileBase::DIRECT;
        break;
    case DiskConfig::DIRECT_ON:
        mode |= FileBase::DIRECT | FileBase::REQUIRE_DIRECT;
        break;
    }

    // automatically enumerate disks as separate device ids

    if (config.device_id == FileBase::DEFAULT_DEVICE_ID)
    {
        config.device_id = Config::GetInstance()->get_next_device_id();
    }
    else
    {
        Config::GetInstance()->update_max_device_id(config.device_id);
    }

    // *** Select fileio Implementation

    if (config.io_impl == "syscall")
    {
        UfsFileBase* result =
            new SyscallFile(config.path, mode, config.queue, disk_allocator_id,
                            config.device_id);
        result->lock();

        // if marked as device but file is not -> throw!
        if (config.raw_device && !result->is_device())
        {
            delete result;
            THRILL_THROWS(IoError, "Disk " << config.path << " was expected to "
                          "be a raw block device, but it is a normal file!");
        }

        // if is raw_device -> get size and remove some flags.
        if (result->is_device())
        {
            config.raw_device = true;
            config.size = result->size();
            config.autogrow =
                config.delete_on_exit = config.unlink_on_open = false;
        }

        if (config.unlink_on_open)
            result->unlink();

        return FileBasePtr(result);
    }
    else if (config.io_impl == "memory")
    {
        MemoryFile* result =
            new MemoryFile(config.queue, disk_allocator_id, config.device_id);
        result->lock();
        return FileBasePtr(result);
    }
#if THRILL_HAVE_LINUXAIO_FILE
    // linuxaio can have the desired queue length, specified as queue_length=?
    else if (config.io_impl == "linuxaio")
    {
        // linuxaio_queue is a singleton.
        config.queue = FileBase::DEFAULT_LINUXAIO_QUEUE;

        UfsFileBase* result =
            new LinuxaioFile(config.path, mode, config.queue, disk_allocator_id,
                             config.device_id, config.queue_length);

        result->lock();

        // if marked as device but file is not -> throw!
        if (config.raw_device && !result->is_device())
        {
            delete result;
            THRILL_THROWS(IoError, "Disk " << config.path << " was expected to "
                          "be a raw block device, but it is a normal file!");
        }

        // if is raw_device -> get size and remove some flags.
        if (result->is_device())
        {
            config.raw_device = true;
            config.size = result->size();
            config.autogrow =
                config.delete_on_exit = config.unlink_on_open = false;
        }

        if (config.unlink_on_open)
            result->unlink();

        return FileBasePtr(result);
    }
#endif
#if THRILL_HAVE_MMAP_FILE
    else if (config.io_impl == "mmap")
    {
        UfsFileBase* result =
            new MmapFile(config.path, mode, config.queue, disk_allocator_id,
                         config.device_id);
        result->lock();

        if (config.unlink_on_open)
            result->unlink();

        return FileBasePtr(result);
    }
#endif
#if THRILL_HAVE_WINCALL_FILE
    else if (config.io_impl == "wincall")
    {
        WfsFileBase* result =
            new WincallFile(config.path, mode, config.queue, disk_allocator_id,
                            config.device_id);
        result->lock();
        return FileBasePtr(result);
    }
#endif

    THRILL_THROW(std::runtime_error,
                 "Unsupported disk I/O implementation '"
                 << config.io_impl << "'.");
}

} // namespace io
} // namespace thrill

/******************************************************************************/
