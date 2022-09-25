// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#ifndef JPINTERFACE_HPP
#define JPINTERFACE_HPP

#include "PathNode.h"

extern Int32 jsonPathParse(PathNode **theTree, char *str, ComDiagsArea **diag, CollHeap *heap);

#endif
