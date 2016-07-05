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

#include <iostream>     // TODO: Remove this temporary header.

#include "BitFunnel/Exceptions.h"
#include "ISliceBufferAllocator.h"
#include "LoggerInterfaces/Logging.h"
#include "Shard.h"
#include "Term.h"       // TODO: Remove this temporary include.


namespace BitFunnel
{
    Shard::Shard(IIngestor& ingestor,
                 size_t id,
                 ISliceBufferAllocator& sliceBufferAllocator,
                 size_t sliceBufferSize)
        : m_ingestor(ingestor),
          m_id(id),
          m_sliceBufferAllocator(sliceBufferAllocator),
          // m_slice(new Slice(*this)),
          m_sliceBuffers(new std::vector<void*>()),
          m_sliceCapacity(16), // TODO: use GetCapacityForByteSize.
          m_sliceBufferSize(sliceBufferSize)
    {
        std::cout << "Shard constructor with m_sliceBufferSize " << m_sliceBufferSize << std::endl;
        // m_activeSlice = m_slice.get();
    }


    void Shard::TemporaryAddPosting(Term const & term, DocIndex index)
    {
        std::cout << "  " << index << ": ";
        term.Print(std::cout);
        std::cout << std::endl;

        {
            // TODO: Remove this lock once it is incorporated into the frequency table class.
            std::lock_guard<std::mutex> lock(m_temporaryFrequencyTableMutex);
            m_temporaryFrequencyTable[term]++;
        }
    }


    void Shard::TemporaryPrintFrequencies(std::ostream& out)
    {
        out << "Term frequency table for shard " << m_id << ":" << std::endl;
        for (auto it = m_temporaryFrequencyTable.begin(); it != m_temporaryFrequencyTable.end(); ++it)
        {
            out << "  ";
            it->first.Print(out);
            out << ": "
                << it->second
                << std::endl;
        }
        out << std::endl;
    }


    DocumentHandleInternal Shard::AllocateDocument()
    {
        std::lock_guard<std::mutex> lock(m_slicesLock);
        DocIndex index;
        if (m_activeSlice == nullptr || !m_activeSlice->TryAllocateDocument(index))
        {
            CreateNewActiveSlice();

            LogAssertB(m_activeSlice->TryAllocateDocument(index),
                       "Newly allocated slice has no space.");
        }
        return DocumentHandleInternal(m_activeSlice, index);
    }


    void* Shard::AllocateSliceBuffer()
    {
        std::cout << "AllocateSliceBuffer " << m_sliceBufferSize << std::endl;
        return m_sliceBufferAllocator.Allocate(m_sliceBufferSize);
    }

    // Must be called with m_slicesLock held.
    void Shard::CreateNewActiveSlice()
    {
        Slice* newSlice = new Slice(*this);

        // TODO: recycle oldSlice.
        // std::vector<void*>* oldSlice = m_sliceBuffers;
        std::vector<void*>* const newSlices = new std::vector<void*>(*m_sliceBuffers);
        newSlices->push_back(newSlice->GetSliceBuffer());

        // LogB(Logging::Info,
        //      "Shard",
        //      "Create new slice for shard %u. New slice count is %u.",
        //      m_id,
        //      newSlices->size());

        // Interlocked operation is used because query threads do not take the lock.
        // For query threads, the operation of swapping the list of slice buffers
        // must be atomic.
        m_sliceBuffers = newSlices;

        // newSlices now contains the old list of slice buffers which needs to
        // be scheduled for recycling.

        m_activeSlice = newSlice;

        // TODO: think if this can be done outside of the lock.

        // TODO: implement recycler.
        // std::unique_ptr<IRecyclable> recyclable(
        //                                         new SliceListChangeRecyclable(nullptr, oldSlices, GetIndex().GetTokenManager()));
        // GetIndex().GetRecycler().ScheduleRecyling(recyclable);
    }


    IIngestor& Shard::GetIndex() const
    {
        return m_ingestor;
    }


    DocIndex Shard::GetSliceCapacity() const
    {
        return  m_sliceCapacity;
    }


    void Shard::ReleaseSliceBuffer(void* sliceBuffer)
    {
        m_sliceBufferAllocator.Release(sliceBuffer);
    }
}
