/*******************************************************************************
 * thrill/api/parquet.hpp
 *
 * DIANode for reading a column from Apache Parquet
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2018 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_PARQUET_HEADER
#define THRILL_API_PARQUET_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>

#if THRILL_HAVE_PARQUET
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <parquet/api/reader.h>
#include <parquet/api/schema.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#endif // THRILL_HAVE_PARQUET

#include <sstream>
#include <type_traits>

namespace thrill {
namespace api {

namespace _detail {
#if THRILL_HAVE_PARQUET
template <typename T> struct PReader
{ using type = parquet::ByteArrayReader; };
template<> struct PReader<bool>    { using type = parquet::BoolReader; };
template<> struct PReader<int32_t> { using type = parquet::Int32Reader; };
template<> struct PReader<int64_t> { using type = parquet::Int64Reader; };
template<> struct PReader<float>   { using type = parquet::FloatReader; };
template<> struct PReader<double>  { using type = parquet::DoubleReader; };

template <typename T> struct arrow_array_type
{ using type = arrow::UInt8Array; };
template<> struct arrow_array_type<bool>    { using type = arrow::UInt8Array; };
template<> struct arrow_array_type<uint8_t> { using type = arrow::UInt8Array; };
template<> struct arrow_array_type<int32_t> { using type = arrow::Int32Array; };
template<> struct arrow_array_type<int64_t> { using type = arrow::Int64Array; };
template<> struct arrow_array_type<float>   { using type = arrow::FloatArray; };
template<> struct arrow_array_type<double>  { using type = arrow::DoubleArray; };
//template<> struct arrow_array_type<std::string>  { using type = arrow::StringArray; };
#else
// dummy
template <typename T> struct PReader { using type = void; };
#endif // THRILL_HAVE_PARQUET
} // namespace _detail

/*!
 * A DIANode which reads data from a single column of an Apache Parquet file.
 *
 * \tparam ValueType Output type of the new DIA
 * \tparam InputType Type of column data, in case conversion is necessary.
 *                   Must be convertible to ValueType.
 * \ingroup api_layer
 */
template <typename ValueType, typename InputType = ValueType>
class ParquetNode final : public SourceNode<ValueType>
{
public:
    static constexpr bool debug = true;
    using Super = SourceNode<ValueType>;
    using Super::context_;

    static_assert(std::is_convertible<InputType, ValueType>::value,
                  "ParquetNode: InputType is not convertible to ValueType");

    using value_reader_t = typename _detail::PReader<InputType>::type;

    /*!
     * Constructor for a ParquetNode. Sets the Context, parents, and parquet
     * column index.
     */
    ParquetNode(Context& ctx,
                const std::string& filename,
                size_t column_index,
                size_t batch_size)
        : Super(ctx, "Parquet"),
          filename_(filename),
          column_index_(column_index),
          batch_size_(batch_size) {
        LOG << "Creating ParquetNode(" << filename << ", " << column_index
            << ", " << batch_size << ")";
    }

    void PushData(bool /* consume */) final {
#if THRILL_HAVE_PARQUET
        // Create a ParquetReader instance
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
            parquet::ParquetFileReader::OpenFile(filename_, false);

        // Get the File MetaData
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

        // Get the number of RowGroups
        int num_row_groups = file_metadata->num_row_groups();
        // assert(num_row_groups == 1);

        // Get the number of Columns
        int num_columns = file_metadata->num_columns();
        // assert(num_columns == 8);

        LOG << "ParquetNode::PushData: got " << num_row_groups << " row groups"
            << " and " << num_columns << " columns";

        std::vector<InputType> buffer(batch_size_);

        // Read every context_.num_workers()-th row group
        const auto my_rank = context_.my_rank(),
            num_workers = context_.num_workers();
        for (int r = my_rank; r < num_row_groups; r += num_workers) {
            LOG << "Reading row group " << r + 1 << " of " << num_row_groups;

            // Get the RowGroup Reader
            std::shared_ptr<parquet::RowGroupReader> row_group_reader =
                parquet_reader->RowGroup(r);

            std::shared_ptr<parquet::ColumnReader> column_reader;

            column_reader = row_group_reader->Column(column_index_);
            value_reader_t* value_reader =
                static_cast<value_reader_t*>(column_reader.get());

            int64_t values_read(0), rows_read(0);
            while (value_reader->HasNext()) {
                // XXX TODO handle null values, ReadBatch apparently doesn't
                // really do that.  We should use the Parquet arrow interface
                // instead of the low-level interface.
                rows_read = value_reader->ReadBatch(
                    batch_size_, nullptr, nullptr, buffer.data(), &values_read);
                sLOG << "Got" << rows_read << "levels," << values_read
                     << "values, requested up to" << batch_size_
                     << "from row group" << r + 1 << "of" << num_row_groups;

                for (int64_t i = 0; i < values_read; ++i) {
                    this->PushItem(static_cast<ValueType>(buffer[i]));
                }
            }
        }
#else
        tlx::die_with_message("This version of Thrill does not include support \
 for Apache Parquet. Please recompile with THRILL_USE_PARQUET.");
#endif // THRILL_HAVE_PARQUET
    }

private:
    //! Input filename
    std::string filename_;
    //! Which column to read
    size_t column_index_;
    //! Number of vaules to read at a time
    size_t batch_size_;
};
/*!
 * A DIANode which reads data from a single column of an Apache Parquet file
 * into an Apache Arrow Table
 *
 * \ingroup api_layer
 */
template <typename ValueType>
class ParquetArrowNode final : public SourceNode<ValueType>
{
public:
    static constexpr bool debug = true;
    using Super = SourceNode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a ParquetNode. Sets the Context, parents, and parquet
     * filename.
     */
    ParquetArrowNode(Context& ctx,
                     const std::string& filename,
                     int column_index)
        : Super(ctx, "Parquet"),
          filename_(filename),
          column_index_(column_index) {
        LOG << "Creating ParquetArrowNode(" << filename << ")";
    }

    void PushData(bool /* consume */) final {
#if THRILL_HAVE_PARQUET
        const auto my_rank = context_.my_rank(),
            num_workers = context_.num_workers();

        auto reader = parquet::ParquetFileReader::OpenFile(filename_);
        auto meta = reader->metadata();

        const auto num_row_groups = meta->num_row_groups();

        sLOG << "ParquetArrowNode::PushData: file" << filename_ << "has size"
             << meta->size() << "with" << meta->num_columns() << "columns,"
             << meta->num_rows() << "rows, and"
             << num_row_groups << "row groups";

        if (debug && my_rank == 0) {
            std::stringstream s;
            parquet::schema::PrintSchema(meta->schema()->schema_root().get(), s);
            LOG << "ParquetArrowNode::PushData: schema: " << s.str();
        }

        parquet::arrow::FileReader filereader(arrow::default_memory_pool(),
                                              std::move(reader));

        std::shared_ptr<arrow::Table> table;

        std::vector<int> indices = { column_index_ };

        // Read every context_.num_workers()-th row group
        for (int r = my_rank; r < num_row_groups; r += num_workers) {
            LOG << "Reading row group " << r + 1 << " of " << num_row_groups
                << " on worker " << my_rank << " of " << num_workers;

            filereader.ReadRowGroup(r, indices, &table);

            sLOG << "Got table with" << table->num_columns() << "columns and"
                 << table->num_rows() << "rows from row group " << r + 1;
            assert(table->num_columns() == 1);

            auto chunks = table->column(0)->data();
            for (int chunk_id = 0; chunk_id < chunks->num_chunks(); ++chunk_id) {
                std::shared_ptr<arrow::Array> chunk = chunks->chunk(chunk_id);
                auto values = std::static_pointer_cast<
                    typename _detail::arrow_array_type<ValueType>::type
                    >(chunk);

                LOG << "Reading chunk " << chunk_id << " of " << chunks->num_chunks()
                    << " has " << values->length() << " values";

                for (int64_t i = 0; i < values->length(); ++i) {
                    this->PushItem(static_cast<ValueType>(values->Value(i)));
                }
            }
        }
#else
        tlx::die_with_message("This version of Thrill does not include support \
 for Apache Parquet. Please recompile with THRILL_USE_PARQUET.");
#endif // THRILL_HAVE_PARQUET
    }

private:
    //! Input filename
    std::string filename_;
    int column_index_;
};

/*!
 * ReadParquet is a Source-DOp, which reads a column from an Apache Parquet file into a DIA
 *
 * \tparam ValueType Type of the output DIA's values
 *
 * \tparam InputType Type of data stored in the Parquet column, defaults to
 * ValueType.  Must be trivially convertible to InputType.  Required e.g. to
 * read unsigned integers (Parquet only knows signed integer types), etc.
 *
 * \param ctx Reference to the Context object
 *
 * \param filename Input filename
 *
 * \param column_idx Index of column to be read
 *
 * \param batch_size Number of values to be read at a time (default: 128)
 *
 * \ingroup dia_sources
 */
template <typename ValueType, typename InputType = ValueType>
auto ReadParquet(Context& ctx, const std::string& filename, int64_t column_idx,
                 int64_t batch_size = 128) {

    using ParquetNode =
        api::ParquetNode<ValueType, InputType>;

    auto node = tlx::make_counting<ParquetNode>(ctx, filename, column_idx,
                                                batch_size);

    return DIA<ValueType>(node);
}

template <typename ValueType>
auto ReadParquetArrow(Context& ctx, const std::string& filename, int column_index) {
    using ParquetArrowNode = api::ParquetArrowNode<ValueType>;

    auto node = tlx::make_counting<ParquetArrowNode>(ctx, filename, column_index);

    return DIA<ValueType>(node);
}

} // namespace api

//! imported from api namespace
using api::ReadParquet;
using api::ReadParquetArrow;

} // namespace thrill

#endif // !THRILL_API_PARQUET_HEADER

/******************************************************************************/
