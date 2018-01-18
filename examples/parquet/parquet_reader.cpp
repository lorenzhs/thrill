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

    if (!clp.process(argc, argv))
        return -1;

    api::ReadParquetTable(filename);

    return api::Run(
        [filename, column_index](api::Context& ctx) {
            // Read with low-level interface
            auto num_values = ReadParquet<int32_t>(ctx, filename, column_index)
                .Size();
            sLOG1 << "Read" << num_values << "values from column"
                  << column_index << "of" << filename;

            // Read with Arrow interface
            num_values = ReadParquetArrow<int>(ctx, filename)
                .Size();
            sLOG1 << "Read" << num_values << "values from" << filename;
        });
}

/******************************************************************************/
