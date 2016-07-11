#include <string>

#include "BitFunnel/Stream.h"

namespace BitFunnel
{
    static const char* c_clickName = "click";
    static const char* c_clickExperimentalName = "clickexperimental";
    static const char* c_fullName = "full";
    static const char* c_fullExperimentalName = "fullexperimental";
    static const char* c_metaWordName = "metaword";
    static const char* c_nonBodyName = "nonbody";
    static const char* c_invalidName = "invalid";

    namespace Stream
    {
        Stream::Classification StringToClassification(const std::string& s)
        {
            if (s.compare(c_clickName) == 0)
            {
                return Stream::Click;
            }
            else if (s.compare(c_clickExperimentalName) == 0)
            {
                return Stream::ClickExperimental;
            }
            else if (s.compare(c_fullName) == 0)
            {
                return Stream::Full;
            }
            else if (s.compare(c_fullExperimentalName) == 0)
            {
                return Stream::FullExperimental;
            }
            else if (s.compare(c_metaWordName) == 0)
            {
                return Stream::MetaWord;
            }
            else if (s.compare(c_nonBodyName) == 0)
            {
                return Stream::NonBody;
            }
            else
            {
                return Stream::Invalid;
            }
        }


        const char* ClassificationToString(Classification classification)
        {
            switch (classification)
            {
            case Click:
                return c_clickName;
            case ClickExperimental:
                return c_clickExperimentalName;
            case Full:
                return c_fullName;
            case FullExperimental:
                return c_fullExperimentalName;
            case MetaWord:
                return c_metaWordName;
            case NonBody:
                return c_nonBodyName;
            default:
                return c_invalidName;
            }
        }
    }
}
