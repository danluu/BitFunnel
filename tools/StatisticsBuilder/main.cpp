#include <fstream>
#include <iostream>
#include <vector>

#include "BitFunnel/Exceptions.h"
#include "BitFunnel/Index/IConfiguration.h"
#include "BitFunnel/Index/Factories.h"
#include "BitFunnel/Index/IIngestor.h"
#include "BitFunnel/Index/IngestChunks.h"
#include "CmdLineParser/CmdLineParser.h"
// #include "Recycler.h" // TODO: what to do with this?
// #include "SliceBufferAllocator.h" // TODO: what to do with this?


int main()
{
    return 0;
}

// TODO: Fix the code below so that it actually compiles. It has dependencies
// on things in src/Index/src.
// Should we pull everything it depends on into inc?

// namespace BitFunnel
// {

//     // Returns a vector with one entry for each line in the file.
//     static std::vector<std::string> ReadLines(char const * fileName)
//     {
//         std::ifstream file(fileName);

//         std::vector<std::string> lines;
//         std::string line;
//         while (std::getline(file, line)) {
//             lines.push_back(std::move(line));
//         }

//         return lines;
//     }


//     static void LoadAndIngestChunkList(char const * chunkListFileName)
//     {
//         // TODO: Add try/catch around file operations.
//         std::cout << "Loading chunk list file '" << chunkListFileName << "'"
//             << std::endl;
//         std::vector<std::string> filePaths = ReadLines(chunkListFileName);

//         std::unique_ptr<IRecycler> recycler =
//             std::unique_ptr<IRecycler>(new Recycler());

//         // Create dummy SliceBufferAllocator to satisfy interface.
//         std::unique_ptr<ISliceBufferAllocator> sliceBufferAllocator =
//             std::unique_ptr<ISliceBufferAllocator>(
//                  new SliceBufferAllocator(0, 0));

//         std::unique_ptr<IIngestor>
//             ingestor(Factories::CreateIngestor(*recycler,
//                                                *sliceBufferAllocator));

//         // Arbitrary maxGramSize that is greater than 1. For initial tests.
//         // TODO: Choose correct maxGramSize.
//         const size_t maxGramSize = 3;
//         std::unique_ptr<IConfiguration>
//             configuration(Factories::CreateConfiguration(maxGramSize));

//         // TODO: Use correct thread count.
//         size_t threadCount = 1;
//         IngestChunks(filePaths, *configuration, *ingestor, threadCount);
//         ingestor->PrintStatistics();
//     }
// }


// void CreateTestFiles(char const * chunkPath, char const * manifestPath)
// {
//     {
//         std::ofstream chunkStream(chunkPath);

//         char const chunk[] =

//             // First document
//             "Title\0Dogs\0\0"
//             "Body\0Dogs\0are\0man's\0best\0friend.\0\0"
//             "\0"

//             // Second document
//             "Title\0Cat\0Facts\0\0"
//             "Body\0The\0internet\0is\0made\0of\0cats.\0\0"
//             "\0"

//             // Third document
//             "Title\0More\0Cat\0Facts\0\0"
//             "Body\0The\0internet\0really\0is\0made\0of\0cats.\0\0"
//             "\0"

//             // End of corpus
//             "\0";

//         // Write out sizeof(chunk) - 1 bytes to skip the trailing zero in corpus
//         // which is not part of the file format.
//         chunkStream.write(chunk, sizeof(chunk) - 1);
//         chunkStream.close();
//     }

//     {
//         std::ofstream manifestStream(manifestPath);
//         manifestStream << chunkPath << std::endl;
//         manifestStream.close();
//     }
// }


// int main2(int /*argc*/, char** /*argv*/)
// {
//     CreateTestFiles(
//         "/tmp/chunks/Chunk1",
//         "/tmp/chunks/manifest.txt");
//     return 0;
// }


// int main(int argc, char** argv)
// {
//     CmdLine::CmdLineParser parser(
//         "StatisticsBuilder",
//         "Ingest documents and compute statistics about them.");

//     CmdLine::RequiredParameter<char const *> chunkListFileName(
//         "chunkListFileName",
//         "Path to a file containing the paths to the chunk files to be ingested. "
//         "One chunk file per line. Paths are relative to working directory.");

//     parser.AddParameter(chunkListFileName);

//     int returnCode = 0;

//     if (parser.TryParse(std::cout, argc, argv))
//     {
//         try
//         {
//             BitFunnel::LoadAndIngestChunkList(chunkListFileName);
//             returnCode = 0;
//         }
//         catch (...)
//         {
//             std::cout << "Unexpected error.";
//             returnCode = 1;
//         }
//     }
//     else
//     {
//         parser.Usage(std::cout, argv[0]);
//         returnCode = 1;
//     }

//     return returnCode;
// }
