#include <sstream>

#include "Array.h"
#include "ConstructorDestructorCounter.h"
#include "SuiteCpp/UnitTest.h"


namespace BitFunnel
{
    namespace Array2DUnitTest
    {


        TestCase(Initialization)
        {
            ConstructorDestructorCounter::ClearCount();

            const unsigned size1 = 4;
            const unsigned size2 = 17;

            {
                Array2D<ConstructorDestructorCounter> a(size1, size2);

                TestAssert(a.GetSize1() == size1);
                TestAssert(a.GetSize2() == size2);

                TestAssert(ConstructorDestructorCounter::GetConstructorCount() == size1 * size2);
                TestAssert(ConstructorDestructorCounter::GetDestructorCount() == 0);
            }

            TestAssert(ConstructorDestructorCounter::GetDestructorCount() == size1 * size2);
        }


        TestCase(FieldAccess)
        {
            const unsigned size1 = 3;
            const unsigned size2 = 5;

            Array2D<unsigned> a(size1, size2);

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

            Array2D<unsigned> a(size1, size2);

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

            Array2D<unsigned> b(stream);

            TestAssert(b.GetSize1() == size1);
            TestAssert(b.GetSize2() == size2);

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
