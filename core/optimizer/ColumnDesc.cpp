/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ColumnDesc.C
 * Description:  definitions of non-inline methods of classes defined
 *               in the header file ColumnDesc.h
 *
 * Created:      6/10/96
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "optimizer/ColumnDesc.h"

#include "common/ComASSERT.h"
#include "common/ComOperators.h"

// -----------------------------------------------------------------------
// definition(s) of non-inline method(s) of class ColumnDescList
// -----------------------------------------------------------------------

NAString ColumnDescList::getColumnDescListAsString(NABoolean removeIntCorrName) const {
  NAString list;

  for (CollIndex i = 0; i < entries(); i++) {
    if (i != 0)  // not first element in the list
      list += ",";
    const ColRefName &colRefName = at(i)->getColRefNameObj();
    ComASSERT(NOT colRefName.isEmpty());
    list += colRefName.getColRefAsAnsiString(FALSE, FALSE, removeIntCorrName);
  }

  return list;
}

ColumnDesc *ColumnDescList::findColumn(const NAString &colName) const {
  for (CollIndex i = 0; i < entries(); i++) {
    ColumnDesc *current = at(i);
    if (current->getColRefNameObj().getColName() == colName) return current;
  }
  return NULL;
}
