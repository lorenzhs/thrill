/*******************************************************************************
 * examples/parquet/parquet_reader.cpp
 *
 * A simple example that reads a single 32-bit integer column from a parquet
 * file into a DIA.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2018 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/all_gather.hpp>
#include <thrill/api/parquet.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

#include <string>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    std::string filename;
    int column_index;
    clp.add_param_string("filename", filename, "input filename");
    clp.add_param_int("column", column_index, "column index");

    std::vector<int> v1 { { 1, 2, 3 } };
    std::vector<double> v2 { { 1.5, 2.5, 3.5 } };
    std::vector<bool> v3 { { true, false, true } };
    std::vector<std::tuple<int, double, bool>> res =
        api::tuplezip<std::tuple<int, double, bool>>(
            v1.begin(), v1.end(), v2.begin(), v3.begin());

    std::vector<std::tuple<int, double, bool>> res2 =
        api::tuplezip_magic(v1.begin(), v1.end(), v2.begin(), v3.begin());

    LOG1 << res << res2;

    if (!clp.process(argc, argv))
        return -1;

    return api::Run(
        [filename, column_index](api::Context& ctx) {
            /*
            // Read with low-level interface
            auto num_values = ReadParquet<int32_t>(ctx, filename, column_index)
                .Size();
            sLOG1 << "Read" << num_values << "values from column"
                  << column_index << "of" << filename;
            */

            // Read with Arrow interface
            auto data = ReadParquetArrow<double>(ctx, filename, column_index)
                        .AllGather();
            sLOG1 << "Read" << data.size() << "values from" << filename;
            LOG1 << data;

            std::vector<int> indices{ { 6, 0 } };
            auto foo = ReadParquetTable<std::tuple<int64_t, double>>(
                ctx, filename, indices)
                .AllGather();
        });
}

/******************************************************************************/
