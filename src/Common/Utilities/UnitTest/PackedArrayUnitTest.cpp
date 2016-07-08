#include "stdafx.h"

#include "PackedArray.h"
#include "SuiteCpp/UnitTest.h"

namespace BitFunnel
{
    namespace PackedArrayUnitTest
    {
        void RunTest1(unsigned capacity, bool useVirtualAlloc);


        TestCase(HeapAlloc)
        {
            RunTest1(1, false);
            RunTest1(5, false);
            RunTest1(100, false);

            // Try one run with VirtualAlloc().
            RunTest1(100, true);
        }


        TestCase(VirtualAlloc)
        {
            RunTest1(100, true);
        }


        TestCase(RoundTrip)
        {
            // First test that array round trips correctly without virtual alloc.
            unsigned capacity = 1000;
            unsigned bitsPerEntry = 9;
            bool useVirtualAlloc = false;

            PackedArray a(capacity, bitsPerEntry, useVirtualAlloc);
            for (unsigned i = 0 ; i < capacity; ++i)
            {
                a.Set(i, i %512);
            }

            std::stringstream stream;
            a.Write(stream);

            PackedArray b(stream);

            TestAssert(a.GetCapacity() == b.GetCapacity());
            for (unsigned i = 0 ; i < capacity; ++i)
            {
                TestAssert(b.Get(i) == i % 512);
            }

            // TODO: Test that the virtual alloc flag round trips.
        }


        void RunTest1(unsigned capacity, bool useVirtualAlloc)
        {
            for (unsigned bitsPerEntry = 1; bitsPerEntry < 57; ++bitsPerEntry)
            {
                PackedArray packed(capacity, bitsPerEntry, useVirtualAlloc);

                unsigned __int64 modulus = 1ULL << bitsPerEntry;

                for (unsigned i = 0 ; i < capacity; ++i)
                {
                    packed.Set(i, i % modulus);

                    // Verify contents of buffer.
                    for (unsigned j = 0 ; j < capacity; ++j)
                    {
                        unsigned __int64 expected = (j <= i) ? (j % modulus) : 0;
                        unsigned __int64 found = packed.Get(j);

                        // If we have written the index, we expect the value we wrote.
                        TestAssert(expected == found);
                    }
                }
            }
        }
    }
}
