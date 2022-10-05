#ifndef _DISJUNCT_H
#define _DISJUNCT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         disjunct.h
 * Description:  Handling of a single disjunct. A Disjunct object represents
 *               the conjunction of a set of predicates
 *
 * Code location: mdam.C
 *
 * Created:      //96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Class Disjunct
// -----------------------------------------------------------------------
#include "common/Collections.h"
#include "export/NABasicObject.h"

class Disjunct : public NABasicObject {
 public:
  virtual ~Disjunct() {}

  // Mutators:
  virtual void clear() { disjunct_.clear(); }

  virtual void remove(const ValueId &valueId) { disjunct_.remove(valueId); }

  virtual void insertSet(const ValueIdSet &idSet) { disjunct_.insert(idSet); }

  virtual void intersectSet(const ValueIdSet &idSet) { disjunct_.intersectSet(idSet); }

  // const functions:
  NABoolean isEmpty() const { return disjunct_.isEmpty(); }

  virtual void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT, const char *title = "disjunct") const {
    disjunct_.print(ofd, indent, title);
  }

  const ValueIdSet &getAsValueIdSet() const { return disjunct_; }

 private:
  ValueIdSet disjunct_;
};
#endif
// eof
