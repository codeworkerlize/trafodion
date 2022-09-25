// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#ifndef NODEEXPR_H
#define NODEEXPR_H


#ifdef _DEBUG
#include <iosfwd>
using namespace std;
extern   ostream *& DumpJPTree();
#endif


enum JPOperatorType {
    PLUS = 1,
    MINUS = 2,
    MULTIPLY,
    DIVIDE,
    EQUAL,
    NOT_EQUAL,
    JPLESS,
    LESS_EQ,
    GREATER,
    GREATER_EQ,
    AND_OP,
    OR_OP,
    NUMERIC,
    JPSTRING,
    IS_AT,
    IS_TRUE,
    IS_FALSE,
    IS_NULL,
    UNARY_REVERSE
};

class BasicCompnt {
public:
    BasicCompnt(JPOperatorType optype, BasicCompnt *child0,
                                    BasicCompnt *child1)
    {
        opType_ = optype;
        child_[0] = child0;
        child_[1] = child1;
    }
    
    virtual void setChild(BasicCompnt* child0, BasicCompnt* child1)
    {
        child_[0] = child0;
        child_[1] = child1;
    }

    virtual BasicCompnt *getChild(Int32 idx)
    {
        if (idx >= 2)
            return NULL;
        return child_[idx];
    }

    virtual JPOperatorType getOpType()
    {
        return opType_;
    }

#ifdef _DEBUG
    virtual void display();
#endif

private:
    JPOperatorType opType_;
    BasicCompnt *child_[2];
};

//+ - * /
class BiArithOp : public BasicCompnt {
public:
    BiArithOp(JPOperatorType optype, BasicCompnt *child0,
                BasicCompnt *child1)
        : BasicCompnt(optype, child0, child1) {}

#ifdef _DEBUG
    virtual void display();
#endif
};

// > < == != >= <=
class BiRelOp : public BasicCompnt {
public:
    BiRelOp(JPOperatorType optype, BasicCompnt *child0 = NULL,
                BasicCompnt *child1 = NULL)
        : BasicCompnt(optype, child0, child1) {}

#ifdef _DEBUG
    virtual void display();
#endif
};

// && || 
class BiLogicOp : public BasicCompnt {
public:
    BiLogicOp(JPOperatorType optype, BasicCompnt *child0 = NULL,
                BasicCompnt *child1 = NULL)
        : BasicCompnt(optype, child0, child1) {}

#ifdef _DEBUG
    virtual void display();
#endif
};

class UnaryLogicOp : public BasicCompnt {
public:
    UnaryLogicOp(JPOperatorType optype, BasicCompnt *child0,
                    BasicCompnt *child1 = NULL)
        : BasicCompnt(UNARY_REVERSE, child0, child1) {}

#ifdef _DEBUG
    virtual void display();
#endif
};

class NumericCmpnt : public BasicCompnt {
public:
    NumericCmpnt(Int32 value)
        : BasicCompnt(NUMERIC, NULL, NULL),
        num_(value),
        isAt_(false) {}
    
    void setIsAt(NABoolean is_at)
    {
        isAt_ = is_at;
    }

#ifdef _DEBUG
    virtual void display();
#endif

private:
    Int32 num_;
    NABoolean isAt_;
};

//true, false, null
class BoolCmpnt : public BasicCompnt {
public:
    BoolCmpnt(JPOperatorType optype)
        :BasicCompnt(optype, NULL, NULL) {}

#ifdef _DEBUG
    virtual void display();
#endif
};

class StringCmpnt : public BasicCompnt {
public:
    StringCmpnt(NAString *value)
        :BasicCompnt(JPSTRING, NULL, NULL),
        str_(value),
        isAt_(false) {}
    
    StringCmpnt(NABoolean is_at)
        :BasicCompnt(IS_AT, NULL, NULL),
        str_(NULL),
        isAt_(true) {}

#ifdef _DEBUG
    virtual void display();
#endif

private:
    NAString *str_;
    NABoolean isAt_;
};

#endif
