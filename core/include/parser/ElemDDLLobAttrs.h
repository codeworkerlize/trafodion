#ifndef ELEMDDLLOBATTRS_H
#define ELEMDDLLOBATTRS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLobAttrs.h
 * Description:  column LOB attrs
 *
 *
 * Created:
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "exp/ExpLOBenums.h"

class ElemDDLLobAttrs : public ElemDDLNode {
 public:
  ElemDDLLobAttrs(ComLobsStorageType storage) : ElemDDLNode(ELM_LOBATTRS), storage_(storage) {}

  // virtual destructor
  virtual ~ElemDDLLobAttrs() {}

  // cast
  virtual ElemDDLLobAttrs *castToElemDDLLobAttrs() { return this; }

  // methods for tracing
  //	virtual const NAString displayLabel2() const;
  //	virtual const NAString getText() const;

  ComLobsStorageType getLobStorage() { return storage_; }

 private:
  ComLobsStorageType storage_;

};  // class ElemDDLElemDDLLobAttrsAttribute

// this class is used if 'serialized' option is specified for a column.
// If it is specified, then column values stored in seabase/hbase are
// encoded and serialized. This allows hbase filter predicates to be
// passed down to hbase during scan and other operations.
class ElemDDLSeabaseSerialized : public ElemDDLNode {
 public:
  ElemDDLSeabaseSerialized(NABoolean serialized) : ElemDDLNode(ELM_SEABASE_SERIALIZED), serialized_(serialized) {}

  // virtual destructor
  virtual ~ElemDDLSeabaseSerialized() {}

  // cast
  virtual ElemDDLSeabaseSerialized *castToElemDDLSeabaseSerialized() { return this; }

  ComBoolean serialized() { return serialized_; };

 private:
  ComBoolean serialized_;
};  // class ElemDDLSeabaseSerialized

#endif  // ELEMDDLLOBATTRS_H
