#ifndef STMTDDLALTERINDEXHBASEOPTIONS_H
#define STMTDDLALTERINDEXHBASEOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterIndexHBaseOptions.h
 * Description:  class for Alter Table/Index <table-name> HBaseOptions(s)
 *               DDL statements
 *
 *               The methods in this class are defined either in this
 *               header file or in the source file StmtDDLAlter.cpp.
 *
 *
 * Created:      5/5/15
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

//
// The reader may notice a resemblance between this class and
// StmtDDLAlterTableHBaseOptions. In fact, they are identical,
// with two exceptions: 1. the name (of course), and 2. what
// class they inherit from. And the latter difference is essential,
// it turns out, because the productions in the parser cast this
// object to its parent class (sigh). If we had designed the
// classes for the ALTER statement somewhat differently (say,
// making clauses orthogonal to the object being altered, at
// least from a syntax tree perspective), we would not need to
// duplicate classes in the way we had to here.
//

#include "StmtDDLAlterIndex.h"

class ElemDDLHbaseOptions;  // forward reference

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterIndexHBaseOptions;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterIndexHBaseOptions
// -----------------------------------------------------------------------
class StmtDDLAlterIndexHBaseOptions : public StmtDDLAlterIndex {
 public:
  // constructor
  StmtDDLAlterIndexHBaseOptions(ElemDDLHbaseOptions *pHBaseOptions);

  // virtual destructor
  virtual ~StmtDDLAlterIndexHBaseOptions();

  // accessor
  inline ElemDDLHbaseOptions *getHBaseOptions();
  inline const ElemDDLHbaseOptions *getHBaseOptions() const;

  // cast
  virtual StmtDDLAlterIndexHBaseOptions *castToStmtDDLAlterIndexHBaseOptions();

  // method for tracing
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  StmtDDLAlterIndexHBaseOptions();                                                  // DO NOT USE
  StmtDDLAlterIndexHBaseOptions(const StmtDDLAlterIndexHBaseOptions &);             // DO NOT USE
  StmtDDLAlterIndexHBaseOptions &operator=(const StmtDDLAlterIndexHBaseOptions &);  // DO NOT USE

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  void setHBaseOptions(ElemDDLHbaseOptions *pHBaseOptions);

  // Copies the information in the specified HBase
  // Options clause (pointed to by pHBaseOptions)
  // to data member HBaseOptions_ in this object.
  //
  // This method can only be invoked during the
  // construction of this object when the HBase Options
  // clause appears.

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  ElemDDLHbaseOptions *pHBaseOptions_;

};  // class StmtDDLAlterIndexHBaseOptions

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterIndexHBaseOptions
// -----------------------------------------------------------------------

//
// accessors
//

inline const ElemDDLHbaseOptions *StmtDDLAlterIndexHBaseOptions::getHBaseOptions() const { return pHBaseOptions_; }

inline ElemDDLHbaseOptions *StmtDDLAlterIndexHBaseOptions::getHBaseOptions() { return pHBaseOptions_; }

#endif  // STMTDDLALTERINDEXHBASEOPTIONS_H
