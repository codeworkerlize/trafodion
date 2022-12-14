
#ifndef TRIGGERS_H
#define TRIGGERS_H
/* -*-C++-*-
********************************************************************************
*
* File:         Triggers.h
* Description:	Definition of trigger objects and repository of common
*				definitions related to triggers.
* Created:		06/23/98
*
*
*
*******************************************************************************
*/

#include "optimizer/ExecuteIdTrig.h"
#include "common/Collections.h"
#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "common/charinfo.h"
#include "export/NABasicObject.h"
#include "optimizer/ItemConstr.h"

//-----------------------------------------------------------------------------
// classes defined in this file:

class Trigger;
class UpdateColumns;
class TriggerList;
class BeforeAndAfterTriggers;

//-----------------------------------------------------------------------------
// Forward references
class SqlTableOpenInfo;
class StmtDDLCreateTrigger;

//-----------------------------------------------------------------------------
//
// -- Transition Variables OLD/NEW Common Constant Variables
//
#undef INIT_
//#undef  GLOB_
#if ((defined INITIALIZE_OLD_AND_NEW_NAMES) || (defined SQLPARSERGLOBALS__INITIALIZE))
#define INIT_(val) = val
#else
#define INIT_(val)
#endif

// const Globals that use the FUNNY_INTERNAL_IDENT macro should be extern.

// The OLD and NEW transition values.
extern const char OLDCorr[] INIT_(FUNNY_INTERNAL_IDENT("OLD"));  //  OLD@
extern const char NEWCorr[] INIT_(FUNNY_INTERNAL_IDENT("NEW"));  //  NEW@
extern const char OLDAnsi[] INIT_(FUNNY_ANSI_IDENT("OLD"));      // "OLD@"
extern const char NEWAnsi[] INIT_(FUNNY_ANSI_IDENT("NEW"));      // "NEW@"

// -- Triggers-Temporary Table Common Constant Variables
extern const char OLD_COLUMN_PREFIX[] INIT_(FUNNY_INTERNAL_IDENT("OLD"));
extern const char NEW_COLUMN_PREFIX[] INIT_(FUNNY_INTERNAL_IDENT("NEW"));

// TBD: to be eliminated, both function and variable
const char TRIG_TEMP_TABLE_SUFFIX[] = "__TEMP";
// The following two functions are used to convert the name of the subject
// table to the name of its triggers-temporary table and vise versa.
// (This is an "abstraction barrier": The rest of the code should only use
//  these calls, and have no internal knowledge of implementation.  Our
//  eventual goal is to have the two names be identical; this will be done
//  solely by changing the implementation of the following functions.)
NAString subjectNameToTrigTemp(const NAString &subjectTableName);
NAString trigTempToSubjectName(const NAString &trigTempTableName);

const char UNIQUEEXE_COLUMN[] = "@UNIQUE_EXECUTE_ID";
const char UNIQUEEXE_QCOLUMN[] = "\"@UNIQUE_EXECUTE_ID\"";
const char UNIQUEIUD_COLUMN[] = "@UNIQUE_IUD_ID";
const char UNIQUEIUD_QCOLUMN[] = "\"@UNIQUE_IUD_ID\"";

//-----------------------------------------------------------------------------
//
// -- class Trigger

class Trigger : public NABasicObject {
 public:
  // Explicit ctor
  Trigger(const QualifiedName &name, const QualifiedName &subjectTable, ComOperation operation,
          ComActivationTime activation, ComGranularity granularity, ComTimestamp timeStamp, NAString *sqlText,
          CharInfo::CharSet sqlTextCharSet, UpdateColumns *updateCols = NULL)
      : name_(name),
        subjectTable_(subjectTable),
        operation_(operation),
        activation_(activation),
        granularity_(granularity),
        timeStamp_(timeStamp),
        sqlText_(sqlText),
        sqlTextCharSet_(sqlTextCharSet),
        updateCols_(updateCols),
        recursionCounter_(0),
        parsedTrigger_(NULL),
        isVirginCopy_(FALSE) {}

  // copy ctor
  // should not use
  Trigger(const Trigger &other)
      : name_(other.name_),
        subjectTable_(other.subjectTable_),
        operation_(other.operation_),
        activation_(other.activation_),
        granularity_(other.granularity_),
        timeStamp_(timeStamp_),
        updateCols_(other.updateCols_)
  // Should never be called. Supplied only for collections.
  {
    CMPASSERT(FALSE);
  }

  // dtor
  virtual ~Trigger();

  // assignment operator
  // should not use
  Trigger &operator=(const Trigger &other)
  // Should never be called. Supplied only because of collections.
  {
    CMPASSERT(FALSE);
    return *this;
  }

  // equality operator
  NABoolean operator==(const Trigger &other) const;

  // Accessors & mutators

  const inline NAString getTriggerName() const { return name_.getQualifiedNameAsAnsiString(); }
  const inline NAString getSubjectTableName() const { return subjectTable_.getQualifiedNameAsAnsiString(); }

  // used for debugging only - print methods
  inline ComOperation getOperation() const { return operation_; }
  inline NABoolean isBeforeTrigger() const { return (activation_ == COM_BEFORE); }
  inline NABoolean isStatementTrigger() const { return (granularity_ == COM_STATEMENT); }

  inline NABoolean isAfterTrigger() const { return (activation_ == COM_AFTER); }
  inline NABoolean isRowTrigger() const { return (granularity_ == COM_ROW); }

  // MV
  virtual NABoolean isMVImmediate() const { return false; }  // this is a regular trigger

  const inline UpdateColumns *getUpdateColumns() const { return updateCols_; }
  const inline NAString *getSqlText() const { return sqlText_; }
  inline CharInfo::CharSet getSqlTextCharSet() const { return sqlTextCharSet_; }
  inline ComTimestamp getTimeStamp() const { return timeStamp_; }

  // return a COPY of the parsed trigger (RelExpr tree) allocated from the
  // statementHeap. Recursion limit checking is also done here: an error-node
  // is returned and a compile-time warning is generated (if the recursion
  // limit was exceeded)
  virtual RelExpr *getParsedTrigger(BindWA *bindWA);
  StmtDDLCreateTrigger *getCreateTriggerNode() const { return parsedTrigger_; }

  // recursion counter used to enforce the limit on the depth of recursive
  // triggers. Inc/Dec are called from RelExpr::bindChildren.
  inline int getRecursionCounter() const { return recursionCounter_; }
  inline void incRecursionCounter() { recursionCounter_++; }
  inline void decRecursionCounter() { recursionCounter_--; }

  // no longer used
  inline void resetRecursionCounter() { recursionCounter_ = 0; }

  //-------------------------------------------------------------------------
  //				Memory Management and Trigger Persistence
  // A trigger object is always created by the Catman (readTriggerDef).
  // Currently, it's memory is managed as part of the StatementHeap. When
  // Triggers become persistent across statements, it's meomery will be
  // managed as part of the ContextHeap, and then there are cases where the
  // Trigger dtor is called and then it deallocates objects pointed to by the
  // Trigger object as well.
  //
  // Please refer to the "Reusing Trigger Objects" Section of the detailed
  // design document for more information on persistence across statements of
  // trigger objects.
  //
  // the Heap() definition is originally intended to control the
  // heap from which all data stored in the TriggerDB (including all
  // classes and members declared in this file and the TriggerDB itself)
  // will be allocated. In order to make TriggerDB persistant across
  // statements, the definition should have been CmpCommon::contextHeap().
  // However, in this version, some RelExpr and ItemExpr nodes cannot be
  // allocated or copied to/from the ContextHeap, since some ctor and
  // copying code specifies CmpCommon::statementHeap() explicitly, regardless
  // of the parameter given to the overloaded new operator.
  // Therefore, until this is fixed, TriggerDB and its content cannot be
  // allocated from the CmpCommon::contextHeap(), and is not persistent
  // across statements.
  //
  // The Heap() method is the ONLY thing that needs to be changed to activate
  // Trigger persistence.
  //-------------------------------------------------------------------------
  inline static NAMemory *Heap() { return CmpCommon::statementHeap(); }

  // for debug
  void print(ostream &os, const char *indent, const char *title) const;

  inline void setTableId(long id) { tableId_ = id; }
  inline long getTableId() { return tableId_; }

 private:
  StmtDDLCreateTrigger *parseTriggerText() const;

  const QualifiedName name_;
  const QualifiedName subjectTable_;
  const UpdateColumns *updateCols_;

  const ComOperation operation_;        // Insert/Update/Delete (IUD)
  const ComActivationTime activation_;  // Before/After
  const ComGranularity granularity_;    // Row/Statement
  const ComTimestamp timeStamp_;        // creation time

  NAString *sqlText_;
  CharInfo::CharSet sqlTextCharSet_;
  StmtDDLCreateTrigger *parsedTrigger_;
  short recursionCounter_;

  // Is this the result of the first parse, or a copy of the second?
  NABoolean isVirginCopy_;
  long tableId_;
};


//-----------------------------------------------------------------------------
//
// -- class UpdateColumns
//
// Used to implement the match-any semantics of Update triggers: If there is
// an intersection between the set of subject columns and the set of updated
// columns, then the trigger is fired. An internal small hash table of column
// positions is used to implement the intersection. If there are no explicit
// subject columns, then all columns are considered as subject column and the
// case is represented especially by isAllColumns_.
// Notice that Trigger::Heap() is used for memory allocation.

static const short INIT_SUBJECT_COLUMNS_NUMBER = 10;

class UpdateColumns : public NABasicObject {
 public:
  UpdateColumns(NABoolean allColumns) : allColumns_(allColumns) {
    if (!allColumns)
      columns_ = new (Trigger::Heap()) SET(int)(Trigger::Heap());
    else
      columns_ = NULL;
  }

  // Ctor from STOI column list
  UpdateColumns(SqlTableOpenInfo *stoi);

  virtual ~UpdateColumns();

  inline void addColumn(const int column) { columns_->insert(column); }

  // Implements match-any: It is enough that there is a common column in the
  // subject columns and in the updated columns for the relevant trigger to
  // fire.
  NABoolean match(const UpdateColumns &other) const;

  inline NABoolean contains(const int col) const { return columns_->contains(col); }

  inline NABoolean isAllColumns() const { return allColumns_; }

  void markColumnsOnBitmap(unsigned char *bitmap, CollIndex numBytes) const;

  void print(ostream &os, const char *indent, const char *title) const;

 private:
  NABoolean allColumns_;  // no explicit subject columns
  SET(int) * columns_;
};

//-----------------------------------------------------------------------------
//
// -- class TriggerList

class TriggerList : public LIST(Trigger *) {
 public:
  TriggerList(NAMemory *h) : LIST(Trigger *)(h){};

  // returns the triggers whose subject columns intersect the given updateCols
  TriggerList *getColumnMatchingTriggers(const UpdateColumns *updateCols);

  // internally sorts the list by time stamp, so that (*this)[0] is the oldest Trigger
  void sortByTimeStamp();

  // free the memory of all the Trigger objects of the list
  void clearAndDestroy();

  void print(ostream &os, const char *indent, const char *title) const;
};

//-----------------------------------------------------------------------------
//
//
// -- class BeforeAndAfterTriggers
//
// Used only to group together existing triggers according to their activation
// time. Serves as the Value of the triggerDB hash.
//
class BeforeAndAfterTriggers : public NABasicObject {
 public:
  // explicit ctor, with timestamp, just copies pointers
  BeforeAndAfterTriggers(TriggerList *beforeTriggers, TriggerList *afterStatementTriggers,
                         TriggerList *afterRowTriggers, ComTimestamp subjectTableTimestamp,
                         ComPrivilegeChecks doPrivCheck = COM_PRIV_NO_CHECK)
      : beforeTriggers_(beforeTriggers),
        afterStatementTriggers_(afterStatementTriggers),
        afterRowTriggers_(afterRowTriggers),
        subjectTableTimestamp_(subjectTableTimestamp),
        doPrivCheck_(doPrivCheck) {}

  BeforeAndAfterTriggers(TriggerList *beforeTriggers, TriggerList *beforeStatementTriggers,
                         TriggerList *beforeRowTriggers, TriggerList *afterStatementTriggers,
                         TriggerList *afterRowTriggers, int triggerCount, ComTimestamp subjectTableTimestamp,
                         ComPrivilegeChecks doPrivCheck = COM_PRIV_NO_CHECK)
      : beforeTriggers_(beforeTriggers),
        beforeStatementTriggers_(beforeStatementTriggers),
        beforeRowTriggers_(beforeRowTriggers),
        afterStatementTriggers_(afterStatementTriggers),
        afterRowTriggers_(afterRowTriggers),
        triggerCount_(triggerCount),
        subjectTableTimestamp_(subjectTableTimestamp),
        doPrivCheck_(doPrivCheck) {}

  virtual ~BeforeAndAfterTriggers();

  // free the memory of all the lists referenced by this object
  void clearAndDestroy();

  // Accessors
  inline int getTtiggerCount() const { return triggerCount_; }
  inline TriggerList *getBeforeTriggers() const { return beforeTriggers_; }
  inline TriggerList *getBeforeStatementTriggers() const { return beforeStatementTriggers_; }
  inline TriggerList *getBeforeRowTriggers() const { return beforeRowTriggers_; }
  inline TriggerList *getAfterStatementTriggers() const { return afterStatementTriggers_; }
  inline TriggerList *getAfterRowTriggers() const { return afterRowTriggers_; }
  inline ComTimestamp getSubjectTableTimeStamp() const { return subjectTableTimestamp_; }

  int entries() const;

  // initialize lists for insertions
  void addNewAfterStatementTrigger(Trigger *newTrigger);
  void addNewAfterRowTrigger(Trigger *newTrigger);

  // Should never be called. Needed for compilation. We rely on
  // NAHashBucket::contains() being called with a default NULL argument for
  // Values in the triggerDB hash where BeforeAndAfterTriggers are Values.
  NABoolean operator==(const BeforeAndAfterTriggers &other) const {
    CMPASSERT(FALSE);
    return FALSE;
  }

  void print(ostream &os, const char *indent, const char *title) const;

 private:
  int triggerCount_;
  TriggerList *beforeStatementTriggers_;
  TriggerList *beforeRowTriggers_;
  TriggerList *beforeTriggers_;
  TriggerList *afterStatementTriggers_;
  TriggerList *afterRowTriggers_;
  ComTimestamp subjectTableTimestamp_;  // used for validation purposes
  ComPrivilegeChecks doPrivCheck_;
};

#endif /* TRIGGERS_H */
