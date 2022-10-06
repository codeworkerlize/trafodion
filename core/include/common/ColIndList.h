
#ifndef COLINDLIST_H
#define COLINDLIST_H
/* -*-C++-*-
******************************************************************************
*
* File:         ColIndList.h
* Description:  This file is the implementation of ColIndList, a list of longs
*		representing a list of column numbers.
* Created:      01/20/2002
* Language:     C++
*
*
*
******************************************************************************
*/

#include <Collections.h>

//-----------------------------------------------------------------------------
// class ColIndList
// ----------------
// This is a list of longs representing a list of column numbers. It is used
// to represent clustering indexes.
class ColIndList : public LIST(int) {
 public:
  ColIndList() : LIST(int)(NULL) {}  // on C++ heap
  ColIndList(LIST(int) list) : LIST(int)(list, NULL) {}
  virtual ~ColIndList() {}
  NABoolean isPrefixOf(const ColIndList &other) const;
  NABoolean isOrderedPrefixOf(const ColIndList &other) const;
};

#endif
