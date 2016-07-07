#include <chrono> // Used for temporary blocking recycle.
#include <iostream> // TODO:
#include <thread> // Used for temporary blocking recycle.

#include "BitFunnel/Token.h"
#include "LoggerInterfaces/Logging.h"
#include "Recycler.h"
#include "Slice.h"

namespace BitFunnel
{
    //*************************************************************************
    //
    // Recycler.
    //
    //*************************************************************************
    // TODO: put this arbitrary 100 constant somewhere
    Recycler::Recycler()
        : m_queue (std::unique_ptr<BlockingQueue<IRecyclable*>>
                   (new BlockingQueue<IRecyclable*>(100)))
    {
    }


    void Recycler::Run()
    {
        for (;;)
        {
            IRecyclable* item;
            // false indicates queue shutdown.
            if (!m_queue->TryDequeue(item))
            {
                return;
            }
            item->TryRecycle();
        }
    }

    void Recycler::ScheduleRecyling(std::unique_ptr<IRecyclable>& resource)
    {
        // TODO: need to take ownership of resource.

        for (;;)
        {
            if (resource->TryRecycle())
            {
                break;
            }

            // TODO: remove this hack.
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1ms);
        }
    }


    //*************************************************************************
    //
    // SliceListChangeRecyclable.
    //
    //*************************************************************************
    SliceListChangeRecyclable::SliceListChangeRecyclable(Slice* slice,
                                                         std::vector<void*> const * sliceBuffers,
                                                         ITokenManager& tokenManager)
        : m_slice(slice),
          m_sliceBuffers(sliceBuffers),
          m_tokenTracker(tokenManager.StartTracker())
    {
    }


    bool SliceListChangeRecyclable::TryRecycle()
    {
        m_tokenTracker->WaitForCompletion();

        if (m_slice != nullptr)
        {
            // Deleting a Slice invokes its destructor which returns its
            // slice buffer to the allocator.
            delete m_slice;
        }

        delete m_sliceBuffers;

        return true;
    }
}
