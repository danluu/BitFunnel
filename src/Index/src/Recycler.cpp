#include "stdafx.h"

#include <Windows.h>

#include "BitFunnel/AsyncTask.h"                   // AsyncTask is a parent class.
#include "BitFunnel/PrioritizedThreadPool.h"
#include "BitFunnel/Token.h"
#include "LoggerInterfaces/Logging.h"
#include "Recycler.h"
#include "Slice.h"

namespace BitFunnel
{
    //*************************************************************************
    //
    // RecyclerTask is a task for PrioritizedThreadPool which recycles an 
    // associated resource after all of its consumers have been drained.
    //
    //*************************************************************************
    class RecyclerTask : public AsyncTask
    {
    public:
        // Creates a task for a given resource. RecyclerTask takes ownership of
        // the IRecyclable resource.
        RecyclerTask(std::unique_ptr<IRecyclable>& resource);

        // Executes the recycling task.
        virtual void Execute() override;

    private:
        // Resource being recycled.
        std::unique_ptr<IRecyclable> m_resource;
    };


    RecyclerTask::RecyclerTask(std::unique_ptr<IRecyclable>& resource)
        : m_resource(resource.release())
    {
    }


    void RecyclerTask::Execute()
    {
        for (;;)
        {
            if (m_resource->TryRecycle())
            {
                break;
            }

            // Sleeping for 1ms prevents from saturating the recycling thread.
            Sleep(1);
        }
    }


    //*************************************************************************
    //
    // Recycler.
    //
    //*************************************************************************
    Recycler::Recycler(PrioritizedThreadPool& threadPool)
        : m_threadPool(threadPool)
    {
    }


    void Recycler::ScheduleRecyling(std::unique_ptr<IRecyclable>& resource)
    {
        m_threadPool.Invoke(*(new RecyclerTask(resource)));
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
        if (!m_tokenTracker->IsComplete())
        {
            return false;
        }

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