#include "stdafx.h"

#include <Windows.h>        // For VirtualAlloc.

#include "LoggerInterfaces/Logging.h"
#include "SimpleBuffer.h"


namespace BitFunnel
{
    SimpleBuffer::SimpleBuffer(size_t capacity)
        : m_buffer(nullptr)    // nullptr indicates buffer has not yet been allocated.
    {
        AllocateBuffer(capacity);
    }


    SimpleBuffer::~SimpleBuffer()
    {
        FreeBuffer();
    }


    void SimpleBuffer::Resize(size_t capacity)
    {
        FreeBuffer();
        AllocateBuffer(capacity);
    }


    char* SimpleBuffer::GetBuffer() const
    {
        return m_buffer;
    }


    void SimpleBuffer::AllocateBuffer(size_t capacity)
    {
        LogAssertB(m_buffer == nullptr);
        if (capacity >= c_virtualAllocThreshold)
        {
            m_usedVirtualAlloc = true;
            m_buffer = static_cast<char*>(VirtualAlloc(nullptr, capacity, MEM_COMMIT, PAGE_READWRITE));
            LogAssertB(m_buffer != nullptr, "VirtualAlloc() failed.");
        }
        else
        {
            m_usedVirtualAlloc = false;
            m_buffer = new char[capacity];
        }
    }


    void SimpleBuffer::FreeBuffer()
    {
        if (m_buffer != nullptr)
        {
            if (m_usedVirtualAlloc)
            {
                VirtualFree(m_buffer, 0, MEM_RELEASE);
            }
            else
            {
                delete [] m_buffer;
            }

            // Set m_buffer to nullptr in case subsequent code throws and causes the
            // destructor to be called.
            m_buffer = nullptr;
        }
    }
}
