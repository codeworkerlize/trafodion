// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#include "Platform.h"
#include "NABoolean.h"
#include "NAStringDef.h"
#include "CmpCommon.h"
#include "ComDiags.h"

#include "JPInterface.h"

#ifdef _DEBUG
#include <fstream>
ostream * dumpJPTree_ = NULL;
#endif


extern __thread  CollHeap *JPHeap;


NAString* enum_to_string(JPOperatorType optype)
{
    NAString *ret = new(JPHeap) NAString(JPHeap);
    switch (optype) {
        case PLUS:
            *ret = "PLUS";
            break;
        case MINUS:
            *ret = "MINUS";
            break;
        case MULTIPLY:
            *ret = "MULTIPLY";
            break;
        case DIVIDE:
            *ret = "DIVIDE";
            break;
        case EQUAL: 
            *ret = "EQUAL";
            break;
        case NOT_EQUAL:
            *ret = "NOT_EQUAL";
            break;
        case JPLESS:
            *ret = "JPLESS";
            break;
        case LESS_EQ:
            *ret = "LESS_EQ";
            break;
        case GREATER:
            *ret = "GREATER";
            break;
        case GREATER_EQ:
            *ret = "GREATER_EQ";
            break;
        case AND_OP:
            *ret = "AND_OP";
            break;
        case OR_OP:
            *ret = "OR_OP";
            break;
        case NUMERIC:
            *ret = "NUMERIC";
            break;
        case JPSTRING:
            *ret = "JPSTRING";
            break;
        case IS_AT:
            *ret = "IS_AT";
            break;
        case IS_TRUE:
            *ret = "IS_TRUE";
            break;
        case IS_FALSE:
            *ret = "IS_FALSE";
            break;
        case IS_NULL:
            *ret = "IS_NULL";
            break;
        case UNARY_REVERSE: 
            *ret = "UNARY_REVERSE";
            break;
    default:
            *ret  = "UNKNOWN";
            break;
    }
    return ret; 
}


#ifdef _DEBUG

ostream *& DumpJPTree()
{
    const char* fileName = getenv("DUMP_JSONPATH_TREE");

    if (NULL == dumpJPTree_ &&
         NULL != fileName)
    {
        
        dumpJPTree_ = new fstream(fileName, ios::out);
    }

    return dumpJPTree_;
}

void BasicCompnt::display()
{

}

void BiArithOp::display()
{
    *DumpJPTree() << "BiArithOp JPOperatorType : " <<  (enum_to_string(getOpType())->data()) << endl;
    if (getChild(0)) {
        *DumpJPTree() << "Left child is: " << endl;
        getChild(0)->display();
    }
    if (getChild(1)) {
        *DumpJPTree() << "Right child is: " << endl;
        getChild(1)->display();
    }
}

void BiRelOp::display()
{
    *DumpJPTree() << "BiRelOp JPOperatorType : " <<  (enum_to_string(getOpType())->data()) << endl;
    if (getChild(0)) {
        *DumpJPTree() << "Left child is: " << endl;
        getChild(0)->display();
    }
    if (getChild(1)) {
        *DumpJPTree() << "Right child is: " << endl;
        getChild(1)->display();
    }
}

void BiLogicOp::display()
{
    *DumpJPTree() << "BiLogicOp JPOperatorType : " <<  (enum_to_string(getOpType())->data()) << endl;
    if (getChild(0)) {
        *DumpJPTree() << "Left child is: " << endl;
        getChild(0)->display();
    }
    if (getChild(1)) {
        *DumpJPTree() << "Right child is: " << endl;
        getChild(1)->display();
    }
}

void UnaryLogicOp::display()
{
    *DumpJPTree() << "UnaryLogicOp JPOperatorType : " <<  (enum_to_string(getOpType())->data()) << endl;
    if (getChild(0)) {
        *DumpJPTree() << "Left child is: " << endl;
        getChild(0)->display();
    }
    if (getChild(1)) {
        *DumpJPTree() << "Right child is: " << endl;
        getChild(1)->display();
    }
}

void NumericCmpnt::display()
{
    *DumpJPTree() << "NumericCmpnt : " <<  (enum_to_string(getOpType())->data()) << endl;
    if (isAt_)
        *DumpJPTree() << "num Component value : @" << endl;
    else
        *DumpJPTree() << "num Component value : " << num_ << endl;
}

void BoolCmpnt::display()
{
    *DumpJPTree() << "BoolCmpnt : " << (enum_to_string(getOpType())->data()) << endl;
}

void StringCmpnt::display()
{
    *DumpJPTree() << "StringCmpnt : " << (enum_to_string(getOpType())->data()) << endl;
    if (isAt_)
        *DumpJPTree() << "str Component value : @" << endl;
    else
        *DumpJPTree() << "str Component value : " << str_ << endl;  
}
#endif

