#ifndef STMTDDLALTERTABLEHBASEOPTIONS_H
#define STMTDDLALTERTABLEHBASEOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterTableHBaseOptions.h
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

#include "StmtDDLAlterTable.h"

class ElemDDLHbaseOptions;  // forward reference

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLAlterTableHBaseOptions;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class StmtDDLAlterTableHBaseOptions
// -----------------------------------------------------------------------
class StmtDDLAlterTableHBaseOptions : public StmtDDLAlterTable {
 public:
  // constructor
  StmtDDLAlterTableHBaseOptions(ElemDDLHbaseOptions *pHBaseOptions);

  // virtual destructor
  virtual ~StmtDDLAlterTableHBaseOptions();

  // accessor
  inline ElemDDLHbaseOptions *getHBaseOptions();
  inline const ElemDDLHbaseOptions *getHBaseOptions() const;

  // cast
  virtual StmtDDLAlterTableHBaseOptions *castToStmtDDLAlterTableHBaseOptions();

  // method for tracing
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

  StmtDDLAlterTableHBaseOptions();                                                  // DO NOT USE
  StmtDDLAlterTableHBaseOptions(const StmtDDLAlterTableHBaseOptions &);             // DO NOT USE
  StmtDDLAlterTableHBaseOptions &operator=(const StmtDDLAlterTableHBaseOptions &);  // DO NOT USE

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

};  // class StmtDDLAlterTableHBaseOptions

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterTableHBaseOptions
// -----------------------------------------------------------------------

//
// accessors
//

inline const ElemDDLHbaseOptions *StmtDDLAlterTableHBaseOptions::getHBaseOptions() const { return pHBaseOptions_; }

inline ElemDDLHbaseOptions *StmtDDLAlterTableHBaseOptions::getHBaseOptions() { return pHBaseOptions_; }

#endif  // STMTDDLALTERTABLEHBASEOPTIONS_H
