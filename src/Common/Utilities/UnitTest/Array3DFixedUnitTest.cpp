#include "stdafx.h"

#include <sstream>

#include "Array.h"
#include "ConstructorDestructorCounter.h"
#include "SuiteCpp/UnitTest.h"


namespace BitFunnel
{
    namespace Array3DFixedUnitTest
    {
        TestCase(Initialization)
        {
            ConstructorDestructorCounter::ClearCount();

            const unsigned size1 = 4;
            const unsigned size2 = 17;
            const unsigned size3 = 23;

            {
                Array3DFixed<ConstructorDestructorCounter, size1, size2, size3> a;

                TestAssert(ConstructorDestructorCounter::GetConstructorCount() == size1 * size2 * size3);
                TestAssert(ConstructorDestructorCounter::GetDestructorCount() == 0);
            }

            TestAssert(ConstructorDestructorCounter::GetDestructorCount() == size1 * size2 * size3);
        }


        TestCase(FieldAccess)
        {
            const unsigned size1 = 3;
            const unsigned size2 = 5;
            const unsigned size3 = 7;

            Array3DFixed<unsigned, size1, size2, size3> a;

            unsigned counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    for (unsigned z = 0; z < size3; ++z)
                    {
                        a.At(x, y, z) =  counter++;
                    }
                }
            }

            counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    for (unsigned z = 0; z < size3; ++z)
                    {
                        TestAssert(a.At(x, y, z) ==  counter++);
                    }
                }
            }
        }


        TestCase(RoundTrip)
        {
            const unsigned size1 = 3;
            const unsigned size2 = 5;
            const unsigned size3 = 7;

            Array3DFixed<unsigned, size1, size2, size3> a;

            unsigned counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    for (unsigned z = 0; z < size3; ++z)
                    {
                        a.At(x, y, z) =  counter++;
                    }
                }
            }

            std::stringstream stream;
            a.Write(stream);

            Array3DFixed<unsigned, size1, size2, size3> b(stream);

            counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    for (unsigned z = 0; z < size3; ++z)
                    {
                        TestAssert(b.At(x, y, z) ==  counter++);
                    }
                }
            }
        }
    }
}
