#include "stdafx.h"

#include <sstream>

#include "Array.h"
#include "ConstructorDestructorCounter.h"
#include "SuiteCpp/UnitTest.h"


namespace BitFunnel
{
    namespace Array2DFixedUnitTest
    {
        TestCase(Initialization)
        {
            ConstructorDestructorCounter::ClearCount();

            const unsigned size1 = 4;
            const unsigned size2 = 17;

            {
                Array2DFixed<ConstructorDestructorCounter, size1, size2> a;

                TestAssert(ConstructorDestructorCounter::GetConstructorCount() == size1 * size2);
                TestAssert(ConstructorDestructorCounter::GetDestructorCount() == 0);
            }

            TestAssert(ConstructorDestructorCounter::GetDestructorCount() == size1 * size2);
        }


        TestCase(FieldAccess)
        {
            const unsigned size1 = 3;
            const unsigned size2 = 5;

            Array2DFixed<unsigned, size1, size2> a;

            unsigned counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    a.At(x, y) =  counter++;
                }
            }

            counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    TestAssert(a.At(x, y) ==  counter++);
                }
            }
        }


        TestCase(RoundTrip)
        {
            const unsigned size1 = 3;
            const unsigned size2 = 5;

            Array2DFixed<unsigned, size1, size2> a;

            unsigned counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    a.At(x, y) =  counter++;
                }
            }

            std::stringstream stream;
            a.Write(stream);

            Array2DFixed<unsigned, size1, size2> b(stream);

            counter = 0;
            for (unsigned x = 0; x < size1; ++x)
            {
                for (unsigned y = 0; y < size2; ++y)
                {
                    TestAssert(b.At(x, y) ==  counter++);
                }
            }
        }
    }
}
