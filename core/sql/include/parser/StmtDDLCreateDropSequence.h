
#ifndef STMTDDLCREATEDROPSEQUENCE_H
#define STMTDDLCREATEDROPSEQUENCE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateDropSequence.h
 * Description:  class for Create/Drop Sequence Statement (parser node)
 *
 *
 * Created:      7/18/2014
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"
#include "parser/ElemDDLSGOptions.h"
#include "ItemConstValueArray.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLCreateSequence;
class StmtDDLDropSequence;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Create Sequence statement
// -----------------------------------------------------------------------
class StmtDDLCreateSequence : public StmtDDLNode {
 public:
  // (default) constructor
  StmtDDLCreateSequence(const QualifiedName &seqQualName, ElemDDLSGOptions *pSGOptions = NULL, ComBoolean alter = FALSE,
                        NABoolean ifNotExistsSet = FALSE, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLCreateSequence();

  // cast
  virtual StmtDDLCreateSequence *castToStmtDDLCreateSequence();

  // ---------------------------------------------------------------------
  // accessors
  // ---------------------------------------------------------------------

  // methods relating to parse tree
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline const QualifiedName &getSeqNameAsQualifiedName() const;
  inline QualifiedName &getSeqNameAsQualifiedName();
  inline const NAString getSeqName() const;

  ElemDDLSGOptions *getSGoptions() { return pSGOptions_; }
  const ElemDDLSGOptions *getSGoptions() const { return pSGOptions_; }

  ComBoolean isAlter() { return alter_; }
  NABoolean ifNotExistsSet() { return ifNotExistsSet_; }

  // ---------------------------------------------------------------------
  // mutators
  // ---------------------------------------------------------------------

  // ---------------------------------------------------------------------
  // other public methods
  // ---------------------------------------------------------------------

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // method for collecting information
  void synthesize();

  // collects information in the parse sub-tree and
  // copy/move them to the current parse node.

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // please do not use the following methods
  //

  StmtDDLCreateSequence();                                          // DO NOT USE
  StmtDDLCreateSequence(const StmtDDLCreateSequence &);             // DO NOT USE
  StmtDDLCreateSequence &operator=(const StmtDDLCreateSequence &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // The syntax of sequence name is
  // [ schema-name . ] seq-name
  QualifiedName seqQualName_;

  ElemDDLSGOptions *pSGOptions_;

  NABoolean alter_;
  NABoolean ifNotExistsSet_;

};  // class StmtDDLCreateSequence

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLCreateSequence
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName &StmtDDLCreateSequence::getSeqNameAsQualifiedName() { return seqQualName_; }

inline const QualifiedName &StmtDDLCreateSequence::getSeqNameAsQualifiedName() const { return seqQualName_; }

inline const NAString StmtDDLCreateSequence::getSeqName() const { return seqQualName_.getQualifiedNameAsAnsiString(); }

// -----------------------------------------------------------------------
// Drop Sequence statement
// -----------------------------------------------------------------------
class StmtDDLDropSequence : public StmtDDLNode {
 public:
  // (default) constructor
  StmtDDLDropSequence(const QualifiedName &seqQualName, ElemDDLNode *pSequenceOptionList = NULL,
                      CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLDropSequence();

  // cast
  virtual StmtDDLDropSequence *castToStmtDDLDropSequence();

  // ---------------------------------------------------------------------
  // accessors
  // ---------------------------------------------------------------------

  // methods relating to parse tree
  virtual int getArity() const;
  virtual ExprNode *getChild(int index);

  inline const QualifiedName &getSeqNameAsQualifiedName() const;
  inline QualifiedName &getSeqNameAsQualifiedName();
  inline const NAString getSeqName() const;

  // ---------------------------------------------------------------------
  // mutators
  // ---------------------------------------------------------------------

  // ---------------------------------------------------------------------
  // other public methods
  // ---------------------------------------------------------------------

  // method for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // method for collecting information
  void synthesize();

  // collects information in the parse sub-tree and
  // copy/move them to the current parse node.

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // please do not use the following methods
  //

  StmtDDLDropSequence();                                        // DO NOT USE
  StmtDDLDropSequence(const StmtDDLDropSequence &);             // DO NOT USE
  StmtDDLDropSequence &operator=(const StmtDDLDropSequence &);  // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  // The syntax of sequence name is
  // [catalog_name.] [ schema-name . ] seq-name
  QualifiedName seqQualName_;

};  // class StmtDDLDropSequence

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropSequence
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName &StmtDDLDropSequence::getSeqNameAsQualifiedName() { return seqQualName_; }

inline const QualifiedName &StmtDDLDropSequence::getSeqNameAsQualifiedName() const { return seqQualName_; }

inline const NAString StmtDDLDropSequence::getSeqName() const { return seqQualName_.getQualifiedNameAsAnsiString(); }

#endif  // STMTDDLCREATEDROPEQUENCE_H
