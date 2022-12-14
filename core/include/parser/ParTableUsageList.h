#ifndef PARTABLEUSAGELIST_H
#define PARTABLEUSAGELIST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ParTableUsageList.h
 * Description:  contains definitions of classes describing table
 *               usages information.
 *
 *
 * Created:      9/12/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "parser/ParNameLocList.h"
#include "common/Collections.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "common/ComSmallDefs.h"
#include "common/NABoolean.h"
#include "optimizer/ObjectNames.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ParTableUsageList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Table Usage List
// -----------------------------------------------------------------------
class ParTableUsageList : private LIST(ExtendedQualName *) {
 public:
  //
  // constructor
  //

  ParTableUsageList(CollHeap *heap = PARSERHEAP());

  // heap specifies the heap to allocate space for objects
  // pointed by the elements in this list.

  //
  // virtual destructor
  //

  virtual ~ParTableUsageList();

  //
  // operators
  //

  inline const ExtendedQualName &operator[](CollIndex index) const;
  inline ExtendedQualName &operator[](CollIndex index);

  //
  // accessors
  //

  inline CollIndex entries() const;

  inline const ExtendedQualName *const find(const ExtendedQualName &tableName) const;
  ExtendedQualName *const find(const ExtendedQualName &tableName);

  //
  // mutator
  //

  NABoolean insert(const ExtendedQualName &usedTableName);

  // inserts usedTableName to the list and returns TRUE if
  // usedTableName is not in the list; otherwise, returns
  // FALSE.

 private:
  //
  // private methods
  //

  ParTableUsageList(const ParTableUsageList &);             // DO NOT USE
  ParTableUsageList &operator=(const ParTableUsageList &);  // DO NOT USE

  //
  // heap to allocate space for objects pointed by elements in the list.
  //
  CollHeap *heap_;

};  // class ParTableUsageList

// -----------------------------------------------------------------------
// definitions of inline methods for class ParTableUsageList
// -----------------------------------------------------------------------

//
// operators
//

inline const ExtendedQualName &ParTableUsageList::operator[](CollIndex index) const {
  return *(LIST(ExtendedQualName *)::operator[](index));
}

inline ExtendedQualName &ParTableUsageList::operator[](CollIndex index) {
  return *(LIST(ExtendedQualName *)::operator[](index));
}

//
// accessors
//

inline CollIndex ParTableUsageList::entries() const { return LIST(ExtendedQualName *)::entries(); }

inline const ExtendedQualName *const ParTableUsageList::find(const ExtendedQualName &tableName) const {
  return ((ParTableUsageList *)this)->find(tableName);
}

#endif  // PARTABLEUSAGELIST_H
