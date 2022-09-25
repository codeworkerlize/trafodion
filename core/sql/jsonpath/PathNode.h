// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#ifndef PATHNODE_H
#define PATHNODE_H


#include "NodeExpr.h"

#define MAX_ARITY 2

class IndexNode {
public:
    IndexNode(Int32 idx);
    IndexNode(Int32 start, Int32 end);
    void setNext(IndexNode *other);
    void setIsStar(NABoolean isstar)
    {
        isStar_ = isstar;   
    }
    IndexNode *getNext()
    {
        return next;
    }

#ifdef _DEBUG
    void display();
#endif

    NABoolean isArrayIdxMatch(Int32);

private:
    NABoolean isStar_;
    Int32 range[2];
    IndexNode *next;
};


class PathNode {

public:
    enum JsonSearchMode {
        NOT_SPECIFIED = 0,
        LAX_MODE = 1,
        STRICT_MODE = 2
    };
    enum PathNodeType {
        PathNodeType_Key = 0,
        PathNodeType_Idx
    };
    PathNode(){};
    PathNode(PathNodeType type, NAString *key, IndexNode *index_node = NULL);
    void addToTree(PathNode *);
    void setFilterExpr(BasicCompnt *f_expr)
    {
        fltExpr_ = f_expr;
    }
    void setNodeKeyWildcard(NABoolean wildc)
    {
        key_wildcard_ = wildc;
    }
    void setJsonMode(JsonSearchMode mode)
    {
        jmode_ = mode;
    }
    PathNode *getChild()
    {
        return child_[0];
    }
    PathNode *getChild(Int32);
    PathNode *getChildLast();

    JsonSearchMode getJsonMode()
    {
        return jmode_;
    }
    NAString getNodeName()
    {
        if (nodeName_)
            return nodeName_->data();
        else
            return "";
    }
    PathNodeType getNodeType() {return type_;}
    IndexNode *getIndex()
    {
        return index_;
    }
    BasicCompnt *getFilter()
    {
        return fltExpr_; 
    }

    int getNodeNum();
    NABoolean isArrayIdxMatch(Int32);
    NABoolean isFieldKeyMatch(char *);
    NABoolean isKeyNode() {return type_ == PathNodeType_Key;}
    NABoolean isIdxNode() {return type_ == PathNodeType_Idx;}

#ifdef _DEBUG
    void display();
#endif

private:
    PathNodeType type_;
    NABoolean key_wildcard_;
    JsonSearchMode jmode_;
    PathNode *child_[MAX_ARITY];
    NAString *nodeName_;
    IndexNode *index_;
    BasicCompnt *fltExpr_;

};

#endif
