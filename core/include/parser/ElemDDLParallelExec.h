
#ifndef ELEMDDLPARALLELEXEC_H
#define ELEMDDLPARALLELEXEC_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLParallelExec.h
 * Description:  class for the parallel execution clause specified in
 *               DDL statements associating with INDEX.
 *
 *
 * Created:      9/18/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "common/NABoolean.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLParallelExec;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// definition of class ElemDDLParallelExec
// -----------------------------------------------------------------------
class ElemDDLParallelExec : public ElemDDLNode {
 public:
  // constructors
  ElemDDLParallelExec(NABoolean parallelExecSpec)
      : ElemDDLNode(ELM_PARALLEL_EXEC_ELEM), parallelExecSpec_(parallelExecSpec), configFileName_(PARSERHEAP()) {}

  ElemDDLParallelExec(NABoolean parallelExecSpec, const NAString &configFileName)
      : ElemDDLNode(ELM_PARALLEL_EXEC_ELEM),
        parallelExecSpec_(parallelExecSpec),
        configFileName_(configFileName, PARSERHEAP()) {
    ComASSERT(parallelExecSpec_ EQU TRUE);
  }

  // virtual destructor
  virtual ~ElemDDLParallelExec();

  // cast
  virtual ElemDDLParallelExec *castToElemDDLParallelExec();

  // accessors
  inline NAString getConfigFileName() const;

  // is parallel execution enabled?
  inline NABoolean isParallelExecEnabled() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  NABoolean parallelExecSpec_;
  NAString configFileName_;

};  // class ElemDDLParallelExec

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLParallelExec
// -----------------------------------------------------------------------

inline NAString ElemDDLParallelExec::getConfigFileName() const { return configFileName_; }

inline NABoolean ElemDDLParallelExec::isParallelExecEnabled() const { return parallelExecSpec_; }

#endif  // ELEMDDLPARALLELEXEC_H
