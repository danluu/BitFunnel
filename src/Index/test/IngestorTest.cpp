// The MIT License (MIT)

// Copyright (c) 2016, Microsoft

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.


#include <future>
#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "Configuration.h"
#include "Document.h"
#include "BitFunnel/Index/Factories.h"
#include "BitFunnel/Index/IIngestor.h"
// #include "BitFunnel/Row.h"
// #include "BitFunnel/Stream.h"
// #include "BitFunnel/TermInfo.h"
#include "DocumentDataSchema.h"
// #include "DocumentHandleInternal.h"
#include "IndexUtils.h"
#include "Ingestor.h"
#include "IRecycler.h"
 #include "MockTermTable.h"
// #include "EmptyTermTable.h"
#include "Recycler.h"
#include "BitFunnel/TermInfo.h"
// #include "Slice.h"
#include "BitFunnel/Term.h"
#include "TrackingSliceBufferAllocator.h"


namespace BitFunnel
{
    class IConfiguration;


    std::vector<std::string> GenerateDocumentText(unsigned docId)
    {
        std::vector<std::string> terms;
        for (int i = 0; i < 32 && docId != 0; ++i, docId >>= 1)
        {
            if (docId & 1)
            {
                terms.push_back(std::to_string(i));
            }
        }
        return terms;
    }


    namespace IngestorTest
    {
        const size_t c_maxGramSize = 1;
        const Term::StreamId c_streamId = 0;

        class MyIndex
        {
        public:
            MyIndex()
            {
                DocumentDataSchema schema;
                // Register blobs here.

                const ShardId c_shardId = 0;

                m_recycler =
                    std::unique_ptr<IRecycler>(new Recycler());
                m_recyclerHandle = std::async(std::launch::async, &IRecycler::Run, m_recycler.get());

                m_termTable.reset(new MockTermTable(c_shardId));

                static const DocIndex c_sliceCapacity = Row::DocumentsInRank0Row(1);

                auto terms = GenerateDocumentText(std::numeric_limits<unsigned>::max());
                for (const auto & term : terms)
                {
                    // The third argument is the idf, which is currently ignored.
                    Term tt(Term::ComputeRawHash(term.c_str()), c_streamId, 0);
                    // The second argument is currently ignored.
                    // The third argument is the number of rows, which must be 1 for now because we only support private rows.
                    m_termTable->AddTerm(tt.GetRawHash(), 0, 1);

                }


                const size_t sliceBufferSize = GetBufferSize(c_sliceCapacity, schema, *m_termTable);

                m_allocator = std::unique_ptr<TrackingSliceBufferAllocator>(new TrackingSliceBufferAllocator(sliceBufferSize));

                m_ingestor =
                    Factories::CreateIngestor(schema,
                                              *m_recycler,
                                              *m_termTable,
                                              *m_allocator);
            }

            ~MyIndex()
            {
                m_ingestor->Shutdown();
                m_recycler->Shutdown();
                m_recyclerHandle.wait();

                m_ingestor.reset();
                m_termTable.reset();
                m_recycler.reset();

                m_allocator.reset();
            }

            IIngestor & GetIngestor() const
            {
                return *m_ingestor;
            }

            ITermTable & GetTermTable() const
            {
                return *m_termTable;
            }

        private:
            std::unique_ptr<TrackingSliceBufferAllocator> m_allocator;
            std::unique_ptr<IIngestor> m_ingestor;
            std::unique_ptr<ITermTable> m_termTable;
            std::unique_ptr<IRecycler> m_recycler;
            std::future<void> m_recyclerHandle;
        };


        class SyntheticIndex
        {
        public:
            SyntheticIndex(unsigned documentCount)
              : m_config(c_maxGramSize),
                m_documentCount(documentCount)
            {
                AddDocumentsToIngestor(m_index.GetIngestor(), m_config, m_documentCount);
            }


            IIngestor & GetIngestor() const
            {
                return m_index.GetIngestor();
            }


            void VerifyQuery(unsigned query)
            {
                auto actualMatches = Match(query, m_index);
                auto expectedMatches = Expected(query);
                ASSERT_EQ(actualMatches.size(), expectedMatches.size());
                for (unsigned i = 0; i < actualMatches.size(); ++i)
                {
                    EXPECT_EQ(actualMatches[i], expectedMatches[i]);
                }
            }
        private:
            // Ingests documents from 0..docCount, using a formula that maps those
            // numbers into documents.
            void AddDocumentsToIngestor(IIngestor& ingestor,
                                        IConfiguration const & config,
                                        unsigned docCount)
            {
                for (unsigned i = 0; i < docCount; ++i)
                {
                    std::unique_ptr<IDocument> document(new Document(config));
                    document->OpenStream(c_streamId);
                    auto terms = GenerateDocumentText(i);
                    for (const auto & term : terms)
                    {
                        document->AddTerm(term.c_str());
                    }
                    document->CloseStream();
                    ingestor.Add(i, *document);
                }
            }


            bool DocumentMatchesQuery(unsigned document, unsigned query)
            {
                return (document | query) == document;
            }


            std::vector<unsigned> Expected(unsigned query)
            {
                std::vector<unsigned> results;
                for (unsigned i = 0; i < m_documentCount; ++i)
                {
                    if (DocumentMatchesQuery(i, query))
                    {
                        results.push_back(i);
                    }
                }
                return results;
            }

            std::vector<unsigned> Match(unsigned query, MyIndex const & index)
            {
                uint64_t accumulator = std::numeric_limits<uint64_t>::max();
                std::vector<unsigned> results;
                auto terms = GenerateDocumentText(query);
                for (const auto & text : terms)
                {
                    Term term(Term::ComputeRawHash(text.c_str()), c_streamId, 0);
                    TermInfo termInfo(term, index.GetTermTable());
                    while (termInfo.MoveNext())
                    {
                        const RowId row = termInfo.Current();
                        Shard & shard = index.GetIngestor().GetShard(0);
                        auto rowOffset = shard.GetRowOffset(row);
                        auto sliceBuffers = shard.GetSliceBuffers();
                        auto base = static_cast<char*>(sliceBuffers[0]);
                        auto ptr = base + rowOffset;
                        accumulator &= *reinterpret_cast<uint64_t*>(ptr);
                    }
                }

                for (unsigned i = 0; accumulator != 0; ++i, accumulator >>= 1)
                {
                    if (accumulator & 1)
                    {
                        results.push_back(i);
                    }
                }
                return results;
            }
            Configuration m_config;
            unsigned m_documentCount;
            MyIndex m_index;
        };


        TEST(Ingestor, BasicSelfChecking)
        {
            const int c_documentCount = 64;
            SyntheticIndex index(c_documentCount);

            for (int i = 0; i < c_documentCount + 1; i++)
            {
                index.VerifyQuery(i);
            }
        }


        TEST(Ingestor, DocFrequency)
        {
            const int c_documentCount = 64;
            SyntheticIndex index(c_documentCount);

            index.GetIngestor().PrintStatistics();
        }
    }
}
