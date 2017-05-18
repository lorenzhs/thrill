/*******************************************************************************
 * tests/io/file_io_sizes_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2010 Johannes Singler <singler@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/create_file.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/iostats.hpp>
#include <thrill/io/request_operations.hpp>
#include <thrill/mem/aligned_allocator.hpp>
#include <tlx/string/format_si_iec_units.hpp>

#include <iostream>

static constexpr bool debug = false;

using namespace thrill;

int main(int argc, char** argv) {
    if (argc < 4)
    {
        LOG1 << "Usage: " << argv[0] << " filetype tempfile maxsize";
        return -1;
    }

    size_t max_size = atoi(argv[3]);
    uint64_t* buffer = reinterpret_cast<uint64_t*>(mem::aligned_alloc(max_size));

    try
    {
        io::FileBasePtr file =
            io::CreateFile(
                argv[1], argv[2],
                io::FileBase::CREAT | io::FileBase::RDWR | io::FileBase::DIRECT);
        file->set_size(max_size);

        io::RequestPtr req;
        io::StatsData stats1(*io::Stats::GetInstance());
        for (size_t size = 4096; size < max_size; size *= 2)
        {
            // generate data
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i)
                buffer[i] = i;

            // write
            LOG << tlx::format_iec_units(size) << "B are being written at once";
            req = file->awrite(buffer, 0, size);
            wait_all(&req, 1);

            // fill with wrong data
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i)
                buffer[i] = 0xFFFFFFFFFFFFFFFFull;

            // read again
            LOG << tlx::format_iec_units(size) << "B are being read at once";
            req = file->aread(buffer, 0, size);
            wait_all(&req, 1);

            // check
            bool wrong = false;
            for (uint64_t i = 0; i < size / sizeof(uint64_t); ++i) {
                if (buffer[i] != i)
                {
                    LOG << "Read inconsistent data at position " << i * sizeof(uint64_t);
                    wrong = true;
                    break;
                }
            }

            if (wrong)
                break;
        }
        std::cout << io::StatsData(*io::Stats::GetInstance()) - stats1;

        file->close_remove();
    }
    catch (io::IoError e)
    {
        std::cerr << e.what() << std::endl;
        throw;
    }

    mem::aligned_dealloc(buffer, max_size);

    return 0;
}

/******************************************************************************/
