#include "gtest/gtest.h"

#include "BitFunnel/BitFunnelTypes.h"  // For DocIndex.
#include "DocTableDescriptor.h"
#include "DocumentDataSchema.h"
#include "IndexUtils.h"

namespace BitFunnel
{
    namespace IndexUtilsTest
    {
        TEST(GetBufferSize, Trivial)
        {
            static const DocIndex c_capacityQuanta = 4096;
            static const size_t c_sizeOfSlicePtr = sizeof(void*);
            DocumentDataSchema schema;
            schema.RegisterVariableSizeBlob();
            schema.RegisterFixedSizeBlob(10);

            {
                // When there are no rows, buffer size is driven only by DocTable.
                const size_t expectedBufferSize = DocTableDescriptor::GetBufferSize(c_capacityQuanta * 1, schema) + c_sizeOfSlicePtr;
                EXPECT_EQ(GetEmptyTermTableBufferSize(c_capacityQuanta * 1, { 0, 0, 0, 0, 0, 0, 0 }, schema), expectedBufferSize);
            }

            {
                // 1 row at rank 0.
                const size_t expectedBufferSize = DocTableDescriptor::GetBufferSize(c_capacityQuanta * 1, schema) + 4096 / 8 + c_sizeOfSlicePtr;
                EXPECT_EQ(GetEmptyTermTableBufferSize(c_capacityQuanta * 1, { 1, 0, 0, 0, 0, 0, 0 }, schema), expectedBufferSize);
            }

            {
                // 10 row at rank 0.
                const size_t expectedBufferSize = DocTableDescriptor::GetBufferSize(c_capacityQuanta * 1, schema) + 4096 / 8 * 10 + c_sizeOfSlicePtr;
                EXPECT_EQ(GetEmptyTermTableBufferSize(c_capacityQuanta * 1, { 10, 0, 0, 0, 0, 0, 0 }, schema), expectedBufferSize);
            }

            {
                // 1 row at rank 0, 10 rows at rank 3.
                const size_t expectedBufferSize = DocTableDescriptor::GetBufferSize(c_capacityQuanta * 1, schema) + 4096 / 8 + 4096 / 8 / 8 * 10 + c_sizeOfSlicePtr;
                EXPECT_EQ(GetEmptyTermTableBufferSize(c_capacityQuanta * 1, { 1, 0, 0, 10, 0, 0, 0 }, schema), expectedBufferSize);
            }

            {
                // 20 rows at rank 6.
                const size_t expectedBufferSize = DocTableDescriptor::GetBufferSize(c_capacityQuanta * 2, schema) + 2 * 4096 / 8 / 64 * 20 + c_sizeOfSlicePtr;
                EXPECT_EQ(GetEmptyTermTableBufferSize(c_capacityQuanta * 2, { 0, 0, 0, 0, 0, 0, 20 }, schema), expectedBufferSize);
            }
        }
    }
}