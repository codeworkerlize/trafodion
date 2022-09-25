// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#include "Platform.h"
#include "NABoolean.h"
#include "NAStringDef.h"
#include "CmpCommon.h"

#include "PathNode.h"

IndexNode::IndexNode(Int32 index)
{
    range[0] = index;
    range[1] = index;
    isStar_ = false;
    next = NULL;
}

IndexNode::IndexNode(Int32 n, Int32 m)
{
    if (n <= m) {
        range[0] = n;
        range[1] = m;
    } else {
        range[0] = m;
        range[1] = n;
    }
    isStar_ = false;
    next = NULL;
}

void IndexNode::setNext(IndexNode *other)
{
    if (!other)
        return;
    IndexNode *tmp = this->next;
    this->next = other;
    other->next = tmp;  
}

NABoolean IndexNode::isArrayIdxMatch(Int32 idx)
{
    if (TRUE == isStar_)
        return TRUE;

    if (idx >= range[0] && idx <= range[1])
        return TRUE;

    return FALSE;
}

PathNode::PathNode(PathNodeType type, NAString *key, IndexNode *index_node)
{
    type_ = type;
    nodeName_ = key;
    index_ = index_node;
    jmode_ = NOT_SPECIFIED;
    for (Int32 i = 0; i < MAX_ARITY; ++i) {
        child_[i] = NULL; 
    }
    fltExpr_ = NULL;    
    key_wildcard_ = FALSE;
}

void PathNode::addToTree(PathNode *other)
{
    PathNode *curNode = this;
    while (curNode->child_[0]) {
        curNode = curNode->child_[0];
    }
    curNode->child_[0] = other;
}

int PathNode::getNodeNum()
{
    int ret;
    PathNode *tmp = this;

    for (ret = 1; NULL != tmp->getChild(); ret++)
    {
        tmp = tmp->getChild();
    }

    return ret;
}

NABoolean PathNode::isArrayIdxMatch(Int32 idx)
{
    IndexNode * tmp = this->index_;

    if (type_ != PathNodeType_Idx)
        return FALSE;

    while (NULL != tmp)
    {
        if (TRUE == tmp->isArrayIdxMatch(idx))
        {
            return TRUE;
        }

        tmp = tmp->getNext();
    }

    return FALSE;
}

NABoolean PathNode::isFieldKeyMatch(char *fname)
{
    Int32 result;

    if (type_ != PathNodeType_Key)
        return FALSE;

    if (TRUE == key_wildcard_)
        return TRUE;

    result = nodeName_->compareTo(fname, NAString::exact);

    if (0 == result)
        return TRUE;
    else
        return FALSE;
}

PathNode *PathNode::getChild(Int32 level)
{
    PathNode *node = this;

    for (int i = 0; i < level && node != NULL; i++)
    {
        node = node->getChild();
    }

    return node;
}

PathNode *PathNode::getChildLast()
{
    PathNode *node = this;

    while (NULL != node->getChild())
    {
        node = node->getChild();
    }

    return node;
}


#ifdef _DEBUG

void PathNode::display()
{
    PathNode *curNode = this;

    if (NULL == DumpJPTree())
        return;

    while(curNode) {
        *DumpJPTree() <<  endl;
        *DumpJPTree() << "====================NODE=====================" << endl;
        *DumpJPTree() << "Node adrr : " <<  curNode << endl;
        *DumpJPTree() << "JsonMode  :(0:not specified, 1:lax, 2:strict)" << curNode->getJsonMode() << endl;

        *DumpJPTree() << "Node type :(0:Key node, 1:Index node)" << curNode->getNodeType() << endl;
        if (PathNodeType_Key == curNode->getNodeType())
            *DumpJPTree() << "Node name : " << curNode->getNodeName() << endl;
        else
        {
            IndexNode *idx = curNode->getIndex();
            while (idx) {
                *DumpJPTree() << "*******INDEX INFO******" << endl;
                idx->display();
                idx = idx ->getNext();
            }
        }
        BasicCompnt *filter = curNode->getFilter();
        if (filter) {
            *DumpJPTree() << "******FILTER EXP******" << endl;
            filter->display();
        }   
        curNode = curNode->getChild();
    }

    delete DumpJPTree();
    DumpJPTree() = NULL;
}

void IndexNode::display()
{
    if (isStar_)
        *DumpJPTree() << "Index is star(*)" << endl;
    else
        *DumpJPTree() <<"Index is [" << range[0] << "," << range[1] << "]" << endl;
}

#endif

