
#ifndef TRIGGERDB_H
#define TRIGGERDB_H
/* -*-C++-*-
*************************************************************************
*
* File:         TriggerDB.h
* Description:	Part of the SchemaDB, a hash dictionary of triggers
* Created:		06/23/98
*
*
*
*************************************************************************
*/

#include "optimizer/Triggers.h"

//-----------------------------------------------------------------------------
// classes defined in this file:

class TableOp;
class TriggerDB;

//-----------------------------------------------------------------------------

static const short TRIGGERDB_INIT_SIZE = 20;

// the TriggerDB will be cleaned up if the threashold is exceeded
// this is a simple mechanism to eliminate overflow, when entries in the DB
// are kept across statements in the ContextHeap. <aviv>
static const short TRIGGERDB_THRASHOLD = TRIGGERDB_INIT_SIZE;

//-----------------------------------------------------------------------------
//
// -- class TableOp

class TableOp : public NABasicObject {
 public:
  // explicit ctor
  TableOp(QualifiedName &subjectTable, ComOperation operation) : subjectTable_(subjectTable), operation_(operation) {}

  inline NABoolean operator==(const TableOp &other) const {
    return operation_ == other.operation_ && subjectTable_ == other.subjectTable_;
  }

  // int hash() const;

  void print(ostream &os) const;

  QualifiedName subjectTable_;
  ComOperation operation_;
};

//-----------------------------------------------------------------------------
//
// -- class TriggerDB

class TriggerDB : public NAHashDictionary<TableOp, BeforeAndAfterTriggers> {
 public:
  static int HashFunction(const TableOp &key);

  TriggerDB(CollHeap *heap)
      : NAHashDictionary<TableOp, BeforeAndAfterTriggers>  // enforce uniqueness
        (&HashFunction, TRIGGERDB_INIT_SIZE, TRUE, heap) {}

  // clear the Trigger DB and destroy (free memory) of all entries
  void clearAndDestroy();

  // when triggerDB is allocated from the contextHeap (see Trigger::Heap()),
  // then we must make sure that after each statement, the recursion counter
  // of every Trigger object must be reset to 0, and in case triggerDB grows
  // beyond its THRASHOLD, we need to deallocate it.
  NABoolean cleanupPerStatement();

  // main driver
  BeforeAndAfterTriggers *getTriggers(QualifiedName &subjectTable, ComOperation operation, BindWA *bindWA,
                                      bool allTrigger = false);

  static BeforeAndAfterTriggers *getAllTriggers(QualifiedName &subjectTable, long tableId = -1,
                                                NABoolean useTrigger = FALSE);

  void print(ostream &os) const;

  NABoolean isHiveTable(QualifiedName &name);

  static BeforeAndAfterTriggers *getTriggers(QualifiedName &subjectTable, ComOperation operation);

  static void putTriggers(QualifiedName &subjectTable, ComOperation operation, BeforeAndAfterTriggers *triggers);

  static void removeTriggers(QualifiedName &subjectTable, ComOperation operation);

 protected:
  // This method validates (timestamp check) the return value before
  // returning. NULL is returned if no entry was found, given the key.
  // This function is relevant when allocating the triggerDB from the context
  // heap. A non-valid entry is also removed from the DB .
  // NOTE: using the superclass's getFirstValue will NOT VALIDATE the entry
  BeforeAndAfterTriggers *getValidEntry(const TableOp *key, BindWA *bindWA);
};

#endif /* TRIGGERDB_H */
