
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "BitFunnel/Index/Factories.h"
#include "BitFunnel/Index/IConfiguration.h"
#include "BitFunnel/Index/IIngestor.h"
#include "ChunkTaskProcessor.h"
#include "SliceBufferAllocator.h"
#include "gtest/gtest.h"


namespace BitFunnel
{
    namespace ChunkTaskProcessorUnitTest
    {
        // Sets up and configures a `ChunkTaskProcessor` (according to
        // `filePaths` and `ngramSize`), and passes that task processor to the
        // `test` function, which is intended to contain all of the test logic
        // (i.e., the `ASSERT`s and `EXPECT`s that verify behavior of the task
        // processor.) See tests below for examples.
        static void RunTest(
            std::vector<std::string> const & filePaths,
            size_t ngramSize,
            std::function<void(ChunkTaskProcessor &)> const & test)
        {
            const std::unique_ptr<IConfiguration>
                configuration(Factories::CreateConfiguration(ngramSize));

            // Create dummy SliceBufferAllocator to satisfy interface.
            // TODO: fix constants.
            std::unique_ptr<ISliceBufferAllocator> sliceBufferAllocator =
                std::unique_ptr<ISliceBufferAllocator>(
                    new SliceBufferAllocator(384, 384*16));
            const std::unique_ptr<IIngestor> ingestor(
                Factories::CreateIngestor(*sliceBufferAllocator));

            ChunkTaskProcessor processor(filePaths, *configuration, *ingestor);

            test(processor);
        }


        TEST(ChunkTaskProcessorUnitTest, Trivial)
        {
            // TODO: What's the Bing style for lambda brace placement?
            RunTest({}, 1, [](ChunkTaskProcessor & processor)
            {
                // Throw when `taskId` is the size of the empty array (i.e.,
                // protect against boundary condition errors).
                EXPECT_ANY_THROW(processor.ProcessTask(0));
            });
        }


        TEST(ChunkTaskProcessorUnitTest, TestBadTaskIds)
        {
            const std::vector<std::string> filePaths { "fileThatDoesNotExist" };
            RunTest(filePaths, 3, [](ChunkTaskProcessor & processor) {
                // Throw when we try to read a file with an invalid name.
                EXPECT_ANY_THROW(processor.ProcessTask(0));

                // Throw for generic out-of-bounds errors.
                EXPECT_ANY_THROW(processor.ProcessTask(3));

                // Throw for negative numbers. On most platforms, this will end
                // up being a really large number. NOTE: Clang and GCC don't
                // warn about this case, but MSVC will error out because of the
                // signed/unsigned conversion.
#ifndef _MSC_VER
                EXPECT_ANY_THROW(processor.ProcessTask(-1));
#endif

                // Throw when `taskId` is the size of the array (i.e., protect
                // against boundary condition errors).
                EXPECT_ANY_THROW(processor.ProcessTask(1));
            });
        }
    }
}
