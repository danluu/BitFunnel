#include "stdafx.h"

#include <Windows.h>                // For VirtualAlloc().
#include <memory.h>                 // For memset.

#include "BitFunnel/FileHeader.h"
#include "BitFunnel/StreamUtilities.h"
#include "BitFunnel/Version.h"
#include "LoggerInterfaces/Logging.h"
#include "PackedArray.h"


namespace BitFunnel
{
    const Version PackedArray::c_version(1, 0, 0);


    PackedArray::PackedArray(size_t capacity, unsigned bitsPerEntry, bool useVirtualAlloc)
        : m_useVirtualAlloc(useVirtualAlloc),
          m_capacity(capacity),
          m_bitsPerEntry(bitsPerEntry),
          m_mask((1ULL << bitsPerEntry) - 1)
    {
        LogAssertB(bitsPerEntry > 0);
        LogAssertB(bitsPerEntry <= c_maxBitsPerEntry);

        m_buffer = AllocateBuffer(m_capacity, m_bitsPerEntry, m_useVirtualAlloc);
    }


    PackedArray::PackedArray(std::istream& input)
    {
        FileHeader fileHeader(input);
        LogAssertB(fileHeader.GetVersion().IsCompatibleWith(c_version));

        m_capacity = StreamUtilities::ReadField<unsigned __int64>(input);
        m_bitsPerEntry = StreamUtilities::ReadField<unsigned __int32>(input);
        m_mask = (1ULL << m_bitsPerEntry) - 1;

        unsigned useVirtualAlloc = StreamUtilities::ReadField<unsigned __int32>(input);
        m_useVirtualAlloc = (useVirtualAlloc != 0);

        m_buffer = AllocateBuffer(m_capacity, m_bitsPerEntry, m_useVirtualAlloc);
        StreamUtilities::ReadArray<unsigned __int64>(input, m_buffer, GetBufferSize(m_capacity, m_bitsPerEntry));
    }


    PackedArray::~PackedArray()
    {
        if (m_useVirtualAlloc)
        {
            VirtualFree(m_buffer, 0, MEM_RELEASE);
        }
        else
        {
            delete [] m_buffer;
        }
    }


    void PackedArray::Write(std::ostream& output) const
    {
        FileHeader fileHeader(c_version, "PackedArray");
        fileHeader.Write(output);

        StreamUtilities::WriteField<unsigned __int64>(output, m_capacity);
        StreamUtilities::WriteField<unsigned __int32>(output, m_bitsPerEntry);
        StreamUtilities::WriteField<unsigned __int32>(output, m_useVirtualAlloc ? 1 : 0);
        StreamUtilities::WriteArray<unsigned __int64>(output, m_buffer, GetBufferSize(m_capacity, m_bitsPerEntry));
    }


    size_t PackedArray::GetCapacity() const
    {
        return m_capacity;
    }


    unsigned __int64 PackedArray::Get(size_t index) const
    {
        LogAssertB(index < m_capacity);
        return Get(index, m_bitsPerEntry, m_mask, m_buffer);
    }


    void PackedArray::Set(size_t index, unsigned __int64 value)
    {
        LogAssertB(index < m_capacity);
        Set(index, m_bitsPerEntry, m_mask, m_buffer, value);
    }


    size_t PackedArray::GetBufferSize(size_t capacity, unsigned bitsPerEntry)
    {
        LogAssertB(bitsPerEntry <= c_maxBitsPerEntry);
        LogAssertB(capacity > 0);

        size_t bufferSize = (capacity * bitsPerEntry + 63) >> 6;

        // Allocate one more unsigned __int64 so that we never have a problem
        // when loading 64-bit values that extend beyond the last valid byte.
        bufferSize++;

        return bufferSize;
    }


    unsigned __int64* PackedArray::AllocateBuffer(size_t capacity, unsigned bitsPerEntry, bool useVirtualAlloc)
    {
        LogAssertB(bitsPerEntry <= c_maxBitsPerEntry);
        LogAssertB(capacity > 0);

        size_t bufferSize = GetBufferSize(capacity, bitsPerEntry);

        unsigned __int64* buffer = nullptr;
        if (useVirtualAlloc)
        {
            buffer = (unsigned __int64 *) VirtualAlloc(nullptr, bufferSize * sizeof(unsigned __int64), MEM_COMMIT, PAGE_READWRITE);
            LogAssertB(buffer != nullptr, "RareTermFilter: failed to allocate buffer - error code: %x", GetLastError());
        }
        else
        {
            buffer = new unsigned __int64[bufferSize];
        }

        memset(buffer, 0, bufferSize * sizeof(unsigned __int64));
        return buffer;
    }


    unsigned __int64 PackedArray::Get(size_t index,
                                      unsigned bitsPerEntry,
                                      unsigned __int64 mask,
                                      unsigned __int64* buffer)
    {
        size_t bit = index * bitsPerEntry;
        size_t byte = bit >> 3;
        unsigned bitInByte = bit & 7;

        unsigned __int64 data = *reinterpret_cast<unsigned __int64*>(reinterpret_cast<char*>(buffer) + byte);

        data = data >> bitInByte;
        unsigned __int64 value = data & mask;

        return value;
    }


    void PackedArray::Set(size_t index,
                          unsigned bitsPerEntry,
                          unsigned __int64 mask,
                          unsigned __int64* buffer,
                          unsigned __int64 value)
    {
        size_t bit = index * bitsPerEntry;
        size_t byte = bit >> 3;
        unsigned bitInByte = bit & 7;

        unsigned __int64* ptr = reinterpret_cast<unsigned __int64*>(reinterpret_cast<char*>(buffer) + byte);
        unsigned __int64 data = ~(mask << bitInByte);

        data &= *ptr;

        data |= ((value & mask) << bitInByte);
        *ptr = data;
    }


    unsigned PackedArray::GetMaxBitsPerEntry()
    {
        return c_maxBitsPerEntry;
    }
}
