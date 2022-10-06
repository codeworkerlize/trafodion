
/* -*-C++-*-
******************************************************************************
*
* File:         ColIndList.cpp
* Description:  This file is the implementation of ColIndList, a list of longs
*		representing a list of column numbers.
* Created:      01/20/2002
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ColIndList.h"

//*
//*-------	isPrefixOf
//*---------------------------------------------------
// Checks is this is a prefix of other, in any order.
NABoolean ColIndList::isPrefixOf(const ColIndList &other) const {
  size_t mySize = entries();
  size_t otherSize = other.entries();

  if (mySize > otherSize) {
    return FALSE;
  }

  for (size_t i = 0; i < mySize; i++) {
    if (!contains(other[i])) {
      return FALSE;
    }
  }

  return TRUE;
}

//*
//*-------	isOrderedPrefixOf
//*---------------------------------------------------
// Checks is this is a prefix of other, in the same order.
NABoolean ColIndList::isOrderedPrefixOf(const ColIndList &other) const {
  size_t mySize = entries();
  size_t otherSize = other.entries();

  if (mySize > otherSize) {
    return FALSE;
  }

  for (size_t i = 0; i < mySize; i++) {
    if (this->at(i) != other[i]) {
      return FALSE;
    }
  }

  return TRUE;
}
