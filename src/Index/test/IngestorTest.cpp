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
// #include "Slice.h"
#include "TrackingSliceBufferAllocator.h"


namespace BitFunnel
{
    class IConfiguration;
    namespace IngestorTest
    {
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

                std::unique_ptr<MockTermTable> termTable(new MockTermTable(c_shardId));

                static const DocIndex c_sliceCapacity = Row::DocumentsInRank0Row(1);
                // TODO: add terms to term table.
                const size_t sliceBufferSize = GetBufferSize(c_sliceCapacity, schema, *termTable);

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

                m_ingestor.reset();
                m_termTable.reset();
                m_recycler.reset();

                m_allocator.reset();
            }

            IIngestor & GetIngestor()
            {
                return *m_ingestor;
            }

        private:
            std::unique_ptr<TrackingSliceBufferAllocator> m_allocator;
            std::unique_ptr<IIngestor> m_ingestor;
            std::unique_ptr<ITermTable> m_termTable;
            std::unique_ptr<IRecycler> m_recycler;
            std::future<void> m_recyclerHandle;
        };


        std::vector<std::string> GenerateDocumentText(unsigned docId)
        {
            std::vector<std::string> terms;
            for (int i = 0; i < 32 && docId != 0; ++i)
            {
                if (docId & 1)
                {
                    terms.push_back(std::to_string(i));
                }
                docId >>= 1;
            }
            return terms;
        }

        // Ingests documents from 0..docCount, using a formula that maps those
        // numbers into documents.
        void AddDocumentsToIngestor(IIngestor& ingestor,
                                    IConfiguration const & config,
                                    unsigned docCount)
        {
            const Term::StreamId c_streamId = 0;
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

        TEST(Ingestor, InProgress)
        {
            const size_t c_maxGramSize = 1;

            MyIndex index;
            Configuration config(c_maxGramSize);

            AddDocumentsToIngestor(index.GetIngestor(), config, 2);
        }
    }
}
