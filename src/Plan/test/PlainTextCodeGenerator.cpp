#include "PlainTextCodeGenerator.h"
#include "TextObjectFormatter.h"


namespace BitFunnel
{
    PlainTextCodeGenerator::PlainTextCodeGenerator(std::ostream& output)
        : m_output(output),
          m_label(0)
    {
    }


    void PlainTextCodeGenerator::EmitZeroArg(char const* name)
    {
        Indent();
        m_output << name << "()" << std::endl;
    }

    void PlainTextCodeGenerator::EmitSignedArg(char const* name, int shift)
    {
        Indent();
        m_output << name << "(" << shift << ")" << std::endl;
    }


    void PlainTextCodeGenerator::EmitUnsignedArg(char const* name,
                                                 unsigned shift)
    {
        Indent();
        m_output << name << "(" << shift << ")" << std::endl;
    }


    void PlainTextCodeGenerator::EmitSizeTArg(char const* name,
                                              size_t shift)
    {
        Indent();
        m_output << name << "(" << shift << ")" << std::endl;
    }


    void PlainTextCodeGenerator::EmitRowArg(char const* name,
                                            size_t id,
                                            bool inverted,
                                            size_t rankDelta)
    {
        Indent();
        m_output << name << "(" << id;
        if (inverted)
        {
            m_output << ", true";
        }
        else
        {
            m_output << ", false";
        }
        m_output << ", " << rankDelta;
        m_output << ")" << std::endl;
    }


    void PlainTextCodeGenerator::LoadRow(size_t id, bool inverted, size_t rankDelta)
    {
        EmitRowArg("LoadRow", id, inverted, rankDelta);
    }


    void PlainTextCodeGenerator::AndRow(size_t id, bool inverted, size_t rankDelta)
    {
        EmitRowArg("AndRow", id, inverted, rankDelta);
    }


    void PlainTextCodeGenerator::LeftShiftOffset(size_t shift)
    {
        EmitSizeTArg("LeftShiftOffset", shift);
    }


    void PlainTextCodeGenerator::RightShiftOffset(size_t shift)
    {
        EmitSizeTArg("RightShiftOffset", shift);
    }


    void PlainTextCodeGenerator::IncrementOffset()
    {
        EmitZeroArg("IncrementOffset");
    }


    void PlainTextCodeGenerator::Push()
    {
        EmitZeroArg("Push");
    }


    void PlainTextCodeGenerator::Pop()
    {
        EmitZeroArg("Pop");
    }


    void PlainTextCodeGenerator::AddStack()
    {
        EmitZeroArg("AddStack");
    }


    void PlainTextCodeGenerator::AndStack()
    {
        EmitZeroArg("AndStack");
    }


    void PlainTextCodeGenerator::Constant(int value)
    {
        EmitSignedArg("Constant", value);
    }


    void PlainTextCodeGenerator::MaxStack()
    {
        EmitZeroArg("MaxStack");
    }


    void PlainTextCodeGenerator::Not()
    {
        EmitZeroArg("Not");
    }


    void PlainTextCodeGenerator::OrStack()
    {
        EmitZeroArg("OrStack");
    }


    void PlainTextCodeGenerator::UpdateFlags()
    {
        EmitZeroArg("UpdateFlags");
    }


    void PlainTextCodeGenerator::Report()
    {
        EmitZeroArg("Report");
    }


    PlainTextCodeGenerator::Label PlainTextCodeGenerator::AllocateLabel()
    {
        return m_label++;
    }


    void PlainTextCodeGenerator::PlaceLabel(Label label)
    {
        m_output << "L" << label << ":" << std::endl;
    }


    void PlainTextCodeGenerator::Call(Label label)
    {
        EmitUnsignedArg("Call", label);
    }


    void PlainTextCodeGenerator::Jmp(Label label)
    {
        EmitUnsignedArg("Jmp", label);
    }


    void PlainTextCodeGenerator::Jnz(Label label)
    {
        EmitUnsignedArg("Jnz", label);
    }


    void PlainTextCodeGenerator::Jz(Label label)
    {
        EmitUnsignedArg("Jz", label);
    }


    void PlainTextCodeGenerator::Return()
    {
        EmitZeroArg("Return");
    }

    void PlainTextCodeGenerator::Indent()
    {
        m_output << "    ";
    }
}