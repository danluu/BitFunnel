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

#include <iostream> // TODO: remove.
// #include <csignal>  // TODO: remove this temporary include.

#include "BitFunnel/Exceptions.h"
#include "BitFunnel/Index/IRecycler.h"
#include "BitFunnel/Index/ISliceBufferAllocator.h"
#include "BitFunnel/Index/ITermTable.h"
#include "BitFunnel/Index/Row.h"
#include "BitFunnel/Index/RowIdSequence.h"
#include "BitFunnel/Index/Token.h"
#include "BitFunnel/Term.h"
#include "IRecyclable.h"
#include "LoggerInterfaces/Check.h"
#include "LoggerInterfaces/Logging.h"
#include "Recycler.h"
#include "Rounding.h"
#include "Shard.h"


namespace BitFunnel
{
    // Extracts a RowId used to mark documents as active/soft-deleted.
    static RowId RowIdForActiveDocument(ITermTable const & termTable)
    {
        RowIdSequence rows(termTable.GetDocumentActiveTerm(), termTable);

        auto it = rows.begin();
        if (it == rows.end())
        {
            RecoverableError error("RowIdForDeletedDocument: expected at least one row.");
            throw error;
        }
        const RowId rowId = *it;

        if (rowId.GetRank() != 0)
        {
            RecoverableError error("RowIdForDeletedDocument: soft deleted row must be rank 0..");
            throw error;
        }

        ++it;
        if (it != rows.end())
        {
            RecoverableError error("RowIdForDeletedDocument: expected no more than one row.");
            throw error;

        }

        return rowId;
    }


    Shard::Shard(ShardId id,
                 IRecycler& recycler,
                 ITokenManager& tokenManager,
                 ITermTable const & termTable,
                 IDocumentDataSchema const & docDataSchema,
                 ISliceBufferAllocator& sliceBufferAllocator,
                 size_t sliceBufferSize)
        : m_shardId(id),
          m_recycler(recycler),
          m_tokenManager(tokenManager),
          m_termTable(termTable),
          m_sliceBufferAllocator(sliceBufferAllocator),
          m_documentActiveRowId(RowIdForActiveDocument(termTable)),
          m_activeSlice(nullptr),
          m_sliceBuffers(new std::vector<void*>()),
          m_sliceCapacity(GetCapacityForByteSize(sliceBufferSize,
                                                 docDataSchema,
                                                 termTable)),
          m_sliceBufferSize(sliceBufferSize),
          // TODO: will need one global, not one per shard.
          m_docFrequencyTableBuilder(new DocumentFrequencyTableBuilder())
    {
        const size_t bufferSize =
            InitializeDescriptors(this,
                                  m_sliceCapacity,
                                  docDataSchema,
                                  termTable);

        LogAssertB(bufferSize <= sliceBufferSize,
                   "Shard sliceBufferSize too small.");

        // TODO: remove.
m_debugHashes.insert(6802403625713157731ull);
m_debugHashes.insert(1892876676525370225ull);
m_debugHashes.insert(10268444040767430083ull);
m_debugHashes.insert(15062712965704030892ull);
m_debugHashes.insert(11753912564081085036ull);
m_debugHashes.insert(14821544247608978427ull);
m_debugHashes.insert(13964518420579998636ull);
m_debugHashes.insert(6124212643376659348ull);
m_debugHashes.insert(914910887621025089ull);
m_debugHashes.insert(4794242573319645915ull);
m_debugHashes.insert(15712799156545103138ull);
m_debugHashes.insert(10215337536302442830ull);
m_debugHashes.insert(12585621973702744924ull);
m_debugHashes.insert(12504057022468599532ull);
m_debugHashes.insert(1608043617621884185ull);
m_debugHashes.insert(4454023814546111243ull);
m_debugHashes.insert(8123696774011221762ull);
m_debugHashes.insert(15813272355341469267ull);
m_debugHashes.insert(1215313345411194885ull);
m_debugHashes.insert(3605943695919173516ull);
m_debugHashes.insert(4371210745611891803ull);
m_debugHashes.insert(3304781985039740734ull);
m_debugHashes.insert(4855692728433604921ull);
m_debugHashes.insert(10699569182950920159ull);
m_debugHashes.insert(8177603636405305729ull);
m_debugHashes.insert(6001822067245432821ull);
m_debugHashes.insert(12690678692941055581ull);
m_debugHashes.insert(11380158788729432162ull);
m_debugHashes.insert(330592983594565267ull);
m_debugHashes.insert(11325442832764730472ull);
m_debugHashes.insert(18267044777185742443ull);
m_debugHashes.insert(16255320760065900224ull);
m_debugHashes.insert(6583896268658057170ull);
m_debugHashes.insert(16114529008645340669ull);
m_debugHashes.insert(2126238523222934849ull);
m_debugHashes.insert(13455127305354058956ull);
m_debugHashes.insert(3282257373824034725ull);
m_debugHashes.insert(5659987003778804210ull);
m_debugHashes.insert(18278812507254872425ull);
m_debugHashes.insert(4164039676279802188ull);
m_debugHashes.insert(14729838783151667390ull);
m_debugHashes.insert(12315007937642784254ull);
m_debugHashes.insert(6870238875005365621ull);
m_debugHashes.insert(16161649087628426011ull);
m_debugHashes.insert(3757761778539672197ull);
m_debugHashes.insert(6834420336083636529ull);
m_debugHashes.insert(7161822144704913047ull);
m_debugHashes.insert(12428861314277401021ull);
m_debugHashes.insert(15255172139633144326ull);
m_debugHashes.insert(12148345934830040355ull);
m_debugHashes.insert(2204779787102981792ull);
m_debugHashes.insert(8376626533906817607ull);
m_debugHashes.insert(11450496932616727138ull);
m_debugHashes.insert(426408076495712027ull);
m_debugHashes.insert(11893184868545713857ull);
m_debugHashes.insert(17812411437748600875ull);
m_debugHashes.insert(872384601400798954ull);
m_debugHashes.insert(649571173582214068ull);
m_debugHashes.insert(5565201803567081604ull);
m_debugHashes.insert(13605386290718159884ull);
m_debugHashes.insert(11389444975272226173ull);
m_debugHashes.insert(16176740567682931522ull);
m_debugHashes.insert(9492949731866586522ull);
m_debugHashes.insert(12936178928238637432ull);
m_debugHashes.insert(9064588680764107159ull);
m_debugHashes.insert(11373659223552308932ull);
m_debugHashes.insert(10279857485235489520ull);
m_debugHashes.insert(3729386093914958526ull);
m_debugHashes.insert(9121661949955867821ull);
m_debugHashes.insert(672002584644571710ull);
m_debugHashes.insert(8259394038600795233ull);
m_debugHashes.insert(7995223337546059673ull);
m_debugHashes.insert(5678811791139835946ull);
m_debugHashes.insert(11339003623108308853ull);
m_debugHashes.insert(8040475089725963666ull);
m_debugHashes.insert(10885105889257259779ull);
m_debugHashes.insert(3456830556196710792ull);
m_debugHashes.insert(16043815791259890075ull);
m_debugHashes.insert(16553332868278569647ull);
m_debugHashes.insert(8968418980549040001ull);
m_debugHashes.insert(12964489370711543558ull);
m_debugHashes.insert(14074731845020147085ull);
m_debugHashes.insert(6470571244136123651ull);
m_debugHashes.insert(1649805913259356034ull);
m_debugHashes.insert(1797909131085410441ull);
m_debugHashes.insert(9280042094497414014ull);
m_debugHashes.insert(1136768615164013330ull);
m_debugHashes.insert(1980843537680751173ull);
m_debugHashes.insert(8743673518708663979ull);
m_debugHashes.insert(13984754940461205902ull);
m_debugHashes.insert(10391908053915065010ull);
m_debugHashes.insert(8098480200733053997ull);
m_debugHashes.insert(12458657556968019632ull);
m_debugHashes.insert(690372461770882916ull);
m_debugHashes.insert(9187759113413641805ull);
m_debugHashes.insert(1254922003706760101ull);
m_debugHashes.insert(2540519198840781926ull);
 m_triggered = false;
    }


    Shard::~Shard() {
        delete static_cast<std::vector<void*>*>(m_sliceBuffers);
    }


    DocumentHandleInternal Shard::AllocateDocument(DocId id)
    {
        std::lock_guard<std::mutex> lock(m_slicesLock);
        DocIndex index;
        if (m_activeSlice == nullptr || !m_activeSlice->TryAllocateDocument(index))
        {
            CreateNewActiveSlice();

            LogAssertB(m_activeSlice->TryAllocateDocument(index),
                       "Newly allocated slice has no space.");
        }

        return DocumentHandleInternal(m_activeSlice, index, id);
    }


    void* Shard::AllocateSliceBuffer()
    {
        return m_sliceBufferAllocator.Allocate(m_sliceBufferSize);
    }

    // Must be called with m_slicesLock held.
    void Shard::CreateNewActiveSlice()
    {
        Slice* newSlice = new Slice(*this);

        std::vector<void*>* oldSlices = m_sliceBuffers;
        std::vector<void*>* const newSlices = new std::vector<void*>(*m_sliceBuffers);
        newSlices->push_back(newSlice->GetSliceBuffer());

        m_sliceBuffers = newSlices;
        m_activeSlice = newSlice;

        // TODO: think if this can be done outside of the lock.
        std::unique_ptr<IRecyclable>
            recyclableSliceList(new DeferredSliceListDelete(nullptr,
                                                            oldSlices,
                                                            m_tokenManager));

        m_recycler.ScheduleRecyling(recyclableSliceList);
    }


    /* static */
    DocIndex Shard::GetCapacityForByteSize(size_t bufferSizeInBytes,
                                           IDocumentDataSchema const & schema,
                                           ITermTable const & termTable)
    {
        DocIndex capacity = 0;
        for (;;)
        {
            const DocIndex newSuggestedCapacity = capacity +
                Row::DocumentsInRank0Row(1, termTable.GetMaxRankUsed());
            const size_t newBufferSize =
                InitializeDescriptors(nullptr,
                                      newSuggestedCapacity,
                                      schema,
                                      termTable);
            if (newBufferSize > bufferSizeInBytes)
            {
                break;
            }

            capacity = newSuggestedCapacity;
        }

        LogAssertB(capacity > 0, "Shard with 0 capacity.");

        return capacity;
    }


    DocTableDescriptor const & Shard::GetDocTable() const
    {
        return *m_docTable;
    }


    ptrdiff_t Shard::GetRowOffset(RowId rowId) const
    {
        // LogAssertB(rowId.IsValid(), "GetRowOffset on invalid row.");

        return GetRowTable(rowId.GetRank()).GetRowOffset(rowId.GetIndex());
    }


    RowTableDescriptor const & Shard::GetRowTable(Rank rank) const
    {
        return m_rowTables.at(rank);
    }


    std::vector<void*> const & Shard::GetSliceBuffers() const
    {
        return *m_sliceBuffers;
    }


    ShardId Shard::GetId() const
    {
        return m_shardId;
    }


    DocIndex Shard::GetSliceCapacity() const
    {
        return  m_sliceCapacity;
    }


    RowId Shard::GetDocumentActiveRowId() const
    {
        return m_documentActiveRowId;
    }


    ITermTable const & Shard::GetTermTable() const
    {
        return m_termTable;
    }


    size_t Shard::GetUsedCapacityInBytes() const
    {
        // TODO: does this really need to be locked?
        std::lock_guard<std::mutex> lock(m_slicesLock);
        return m_sliceBuffers.load()->size() * m_sliceBufferSize;
    }


    /* static */
    size_t Shard::InitializeDescriptors(Shard* shard,
                                        DocIndex sliceCapacity,
                                        IDocumentDataSchema const & docDataSchema,
                                        ITermTable const & termTable)
    {
        const Rank maxRank = termTable.GetMaxRankUsed();

        size_t currentOffset = 0;

        // A pointer to a Slice object is placed at the beginning of the slice buffer.
        currentOffset += sizeof(Slice*);

        //
        // DocTable
        //
        currentOffset = RoundUp(currentOffset, DocTableDescriptor::c_docTableByteAlignment);
        if (shard != nullptr)
        {
            // The cast of currentOffset is to avoid an implicit sign conversion
            // warning. It's possible that we should just make currentOffset
            // ptrdiff_t.
            shard->m_docTable.reset(new DocTableDescriptor(sliceCapacity,
                                                           docDataSchema,
                                                           static_cast<ptrdiff_t>(currentOffset)));
        }
        currentOffset += DocTableDescriptor::GetBufferSize(sliceCapacity, docDataSchema);

        //
        // RowTables
        //
        for (Rank rank = 0; rank <= c_maxRankValue; ++rank)
        {
            currentOffset = RoundUp(currentOffset, RowTableDescriptor::c_rowTableByteAlignment);

            const RowIndex rowCount = termTable.GetTotalRowCount(rank);

            if (shard != nullptr)
            {
                shard->m_rowTables.emplace_back(
                    sliceCapacity, rowCount, rank, maxRank, currentOffset);
            }

            currentOffset += RowTableDescriptor::GetBufferSize(
                sliceCapacity, rowCount, rank, maxRank);
        }

        const size_t sliceBufferSize = static_cast<size_t>(currentOffset);

        return sliceBufferSize;
    }


    void Shard::RecycleSlice(Slice& slice)
    {
        std::vector<void*>* oldSlices = nullptr;

        {
            std::lock_guard<std::mutex> lock(m_slicesLock);

            if (!slice.IsExpired())
            {
                throw RecoverableError("Slice being recycled has not been fully expired");
            }

            std::vector<void*>* const newSlices = new std::vector<void*>();
            newSlices->reserve(m_sliceBuffers.load()->size() - 1);

            for (const auto it : *m_sliceBuffers)
            {
                if (it != slice.GetSliceBuffer())
                {
                    newSlices->push_back(it);
                }
            }

            if (m_sliceBuffers.load()->size() != newSlices->size() + 1)
            {
                throw RecoverableError("Slice buffer to be removed is not found in the active slice buffers list");
            }

            oldSlices = m_sliceBuffers.load();
            m_sliceBuffers = newSlices;

            if (m_activeSlice == &slice)
            {
                // If all of the above validations are true, then this was the
                // last Slice in the Shard.
                m_activeSlice = nullptr;
            }
        }

        // Scheduling the Slice and the old list of slice buffers can be
        // done outside of the lock.
        std::unique_ptr<IRecyclable>
            recyclableSliceList(new DeferredSliceListDelete(&slice,
                                                            oldSlices,
                                                            m_tokenManager));

        m_recycler.ScheduleRecyling(recyclableSliceList);
    }


    void Shard::ReleaseSliceBuffer(void* sliceBuffer)
    {
        m_sliceBufferAllocator.Release(sliceBuffer);
    }


    void Shard::AddPosting(Term const & term,
                           DocIndex index,
                           void* sliceBuffer)
    {
        if (m_docFrequencyTableBuilder.get() != nullptr)
        {
            std::lock_guard<std::mutex> lock(m_temporaryFrequencyTableMutex);
            m_docFrequencyTableBuilder->OnTerm(term);
        }


        // if (m_debugHashes.find(term.GetRawHash()) != m_debugHashes.end())
        // {
        //     if (!m_triggered)
        //     {
        //         m_triggered = true;
        //         std::raise(SIGINT);
        //     }
        // }
        RowIdSequence rows(term, m_termTable);

        for (auto const row : rows)
        {
            bool alreadySet =
                m_rowTables[row.GetRank()].GetBit(sliceBuffer,
                                                  row.GetIndex(),
                                                  index);
            if (m_debugHashes.find(term.GetRawHash()) != m_debugHashes.end() &&
                row.GetRank() == 0u)
            {
                if (!m_triggered)
                {
                    m_triggered = true;
                    // std::raise(SIGINT);
                }
                // // Need some lock to avoid output mangling. Using this one because it's handy.
                // std::lock_guard<std::mutex> lock(m_temporaryFrequencyTableMutex);
                // std::cout << row.GetIndex()
                //           << ","
                //           << index
                //           << ",good,"
                //           << alreadySet
                //           << ","
                //           << term.GetRawHash()
                //           << std::endl;
            }
            else if (row.GetIndex() == 6221 && row.GetRank() == 0)
            {
                // Need some lock to avoid output mangling. Using this one because it's handy.
                std::lock_guard<std::mutex> lock(m_temporaryFrequencyTableMutex);
                std::cout << row.GetIndex()
                          << ","
                          << index
                          << ",bad,"
                          << alreadySet
                          << ","
                          << term.GetRawHash()
                          << std::endl;
            }
            m_rowTables[row.GetRank()].SetBit(sliceBuffer,
                                              row.GetIndex(),
                                              index);
        }
    }


    void Shard::AssertFact(FactHandle fact, bool value, DocIndex index, void* sliceBuffer)
    {
        Term term(fact, 0u, 0u, 1u);
        RowIdSequence rows(term, m_termTable);
        auto it = rows.begin();

        if (it == rows.end())
        {
            RecoverableError error("Shard::AssertFact: expected at least one row.");
            throw error;
        }

        const RowId row = *it;

        ++it;
        if (it != rows.end())
        {
            RecoverableError error("Shard::AssertFact: expected no more than one row.");
            throw error;

        }

        RowTableDescriptor const & rowTable =
            m_rowTables[row.GetRank()];

        if (value)
        {
            rowTable.SetBit(sliceBuffer,
                            row.GetIndex(),
                            index);
        }
        else
        {
            rowTable.ClearBit(sliceBuffer,
                              row.GetIndex(),
                              index);
        }
    }


    void Shard::TemporaryRecordDocument()
    {
        m_docFrequencyTableBuilder->OnDocumentEnter();
    }


    void Shard::TemporaryWriteDocumentFrequencyTable(std::ostream& out,
                                                     ITermToText const * termToText) const
    {
        // TODO: 0.0 is the truncation frequency, which shouldn't be fixed at 0.
        m_docFrequencyTableBuilder->WriteFrequencies(out, 0.0, termToText);
    }


    void Shard::TemporaryWriteIndexedIdfTable(std::ostream& out) const
    {
        // TODO: 0.0 is the truncation frequency, which shouldn't be fixed at 0.
        m_docFrequencyTableBuilder->WriteIndexedIdfTable(out, 0.0);
    }


    void Shard::TemporaryWriteCumulativeTermCounts(std::ostream& out) const
    {
        m_docFrequencyTableBuilder->WriteCumulativeTermCounts(out);
    }


    std::vector<double> Shard::GetDensities(Rank rank) const
    {
        // Hold a token to ensure that m_sliceBuffers won't be recycled.
        auto token = m_tokenManager.RequestToken();

        // m_sliceBuffers can change at any time, but we can safely grab a copy
        // because
        //   1. m_sliceBuffers is std::atomic.
        //   2. no m_sliceBuffers value observed while holding token can be
        //      recycled.
        std::vector<void*> const & buffers = *m_sliceBuffers;

        RowTableDescriptor const & rowTable = m_rowTables[rank];
        RowTableDescriptor const & rowTable0 = m_rowTables[0];

        RowIndex active = (*RowIdSequence(m_termTable.GetDocumentActiveTerm(),
                                          m_termTable).begin()).GetIndex();

        std::vector<double> densities;
        for (RowIndex row = 0; row < rowTable.GetRowCount(); ++row)
        {
            size_t activeBitCount = 0;
            size_t setBitCount = 0;
            for (auto buffer : buffers)
            {
                for (DocIndex doc = 0; doc < GetSliceCapacity(); ++doc)
                {
                    // Only count bits for active documents.
                    if (rowTable0.GetBit(buffer, active, doc) == 1)
                    {
                        // This document is active in the index.
                        // Go ahead and include its bits in the density
                        // calculation.
                        ++activeBitCount;
                        setBitCount += rowTable.GetBit(buffer, row, doc);
                    }
                }
            }
            double density =
                (activeBitCount == 0) ?
                0.0 :
                static_cast<double>(setBitCount) / activeBitCount;

            densities.push_back(density);
        }

        return densities;
    }


    // static
    ptrdiff_t Shard::GetSlicePtrOffset()
    {
        // The slice pointer is at the beginning of the buffer.
        return static_cast<ptrdiff_t>(0ull);
    }
}
