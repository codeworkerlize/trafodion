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

#include "jp_parser_def.h"
#include "JPInterface.h"

__thread PathNode *theSearchPathTree = NULL;
__thread char *jp_input = NULL;
static ComDiagsArea *JPDiag = NULL;
__thread  CollHeap *JPHeap = NULL;

extern Int32 yyparse(void*);
extern void resetLexer(void *);
extern void init_scaner (void* &);
extern void destroy_scaner(void* &scaner);

Int32 jsonPathParse(PathNode **jpTree, char *str, ComDiagsArea **diag, CollHeap *heap)
{
    JPHeap = heap;

    if (NULL == JPDiag) {
        JPDiag = CmpCommon::diags();
        if(!JPDiag)
            JPDiag = ::new(JPHeap) ComDiagsArea(JPHeap);
    }
    jp_input = str;
    void* scaner;
    init_scaner (scaner);
    resetLexer(scaner);
    Int32 ret = yyparse(scaner);
    destroy_scaner (scaner);

#ifdef _DEBUG
    if (NULL != theSearchPathTree)
        theSearchPathTree->display();
#endif

    if (0 == ret)
    {
        *jpTree = theSearchPathTree;
    }
    else
    {
        *jpTree = NULL;

        *diag = JPDiag->copy();
        JPDiag->clear();
    }
    return ret;
}

ComDiagsArea *getDiag()
{
    return JPDiag;
}

