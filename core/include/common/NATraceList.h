
#ifndef NATRACELIST_H
#define NATRACELIST_H

/* -*-C++-*-
******************************************************************************
*
* File:         NATraceList.h
* Description:  Procedures for graphical display of expressions,
*               properties, and rules
*
* Created:      08/05/97
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "common/Collections.h"
#include "export/NAStringDef.h"

class NATraceList : public LIST(NAString) {
 public:
  NATraceList() : LIST(NAString)(NULL) {}  // on C++ heap
  ~NATraceList() {}

  inline NATraceList &operator=(const NATraceList &rhs) {
    for (CollIndex i = 0; i < rhs.entries(); i++) {
      append(rhs[i]);
    }
    return *this;
  }
  inline void append(const NATraceList &newStringList) {
    for (CollIndex i = 0; i < newStringList.entries(); i++) {
      append(newStringList[i]);
    }
  }
  inline void append(const NAString &newString) { insertAt(entries(), newString); }
  void append(const NAString &prefix, const NATraceList &newStringList) {
    for (CollIndex i = 0; i < newStringList.entries(); i++) {
      append(prefix + newStringList[i]);
    }
  }
};  // class NATraceList

#endif  // NATRACELIST_H
