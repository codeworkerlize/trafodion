/* -*-C++-*- */

#ifndef STMTDDLALTERROUTINE_H
#define STMTDDLALTERROUTINE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLCreateRoutine.h
 * Description:  class representing Alter Routine Statement parser nodes
 *
 *
 * Created:      11/10/09
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "parser/StmtDDLCreateRoutine.h"

// -----------------------------------------------------------------------
// Alter Routine statement
// -----------------------------------------------------------------------
class StmtDDLAlterRoutine : public StmtDDLCreateRoutine {
 public:
  // initialize constructor
  StmtDDLAlterRoutine(ComAnsiNameSpace eNameSpace, const QualifiedName &aRoutineName, const QualifiedName &anActionName,
                      ComRoutineType eRoutineType, ElemDDLNode *pAlterPassThroughParamParseTree,
                      ElemDDLNode *pAddPassThroughParamParseTree, ElemDDLNode *pRoutineAttributesParseTree,
                      CollHeap *heap);

  // virtual destructor
  virtual ~StmtDDLAlterRoutine();

  // cast
  virtual StmtDDLAlterRoutine *castToStmtDDLAlterRoutine();

  //
  // accessors
  //

  inline const ComAnsiNameSpace getRoutineNameSpace(void) const;

  inline const ElemDDLPassThroughParamDefArray &getAddPassThroughParamArray() const;
  inline ElemDDLPassThroughParamDefArray &getAddPassThroughParamArray();

  inline const ElemDDLPassThroughParamDefArray &getAlterPassThroughParamArray() const;
  inline ElemDDLPassThroughParamDefArray &getAlterPassThroughParamArray();

  // methods relating to parse tree
  inline ElemDDLNode *getAddPassThroughInputsParseTree();
  inline ElemDDLNode *getAlterPassThroughInputsParseTree();

  //
  // mutators
  //

  // methods relating to parse tree
  inline void setAddPassThroughInputsParseTree(ElemDDLNode *pParseTree);
  inline void setAlterPassThroughInputsParseTree(ElemDDLNode *pParseTree);

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *pBindWA);

  //
  // Method for collecting information
  //   Collects information in the parse sub-tree and
  //   copy/move them to the current parse node.
  //

  void synthesize(void);

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1(void) const;
  virtual const NAString displayLabel2(void) const;
  virtual NATraceList getDetailInfo(void) const;
  virtual const NAString getText(void) const;

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  //
  // please do not use the following methods
  //

  StmtDDLAlterRoutine();                                        // DO NOT USE
  StmtDDLAlterRoutine(const StmtDDLAlterRoutine &);             // DO NOT USE
  StmtDDLAlterRoutine &operator=(const StmtDDLAlterRoutine &);  // DO NOT USE
  NABoolean operator==(const StmtDDLAlterRoutine &);            // DO NOT USE

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------
  ComAnsiNameSpace nameSpace_;

  // list of existing pass through parameters whose values are to be changed.
  ElemDDLPassThroughParamDefArray alterPassThroughParamArray_;

  //
  // pointers to child parse nodes
  //

  ElemDDLNode *alterPassThroughInputsParseTree_;

};  // class StmtDDLAlterRoutine

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterRoutine
// -----------------------------------------------------------------------

//
// accessors
//

inline const ComAnsiNameSpace StmtDDLAlterRoutine::getRoutineNameSpace(void) const { return nameSpace_; }

inline const ElemDDLPassThroughParamDefArray &StmtDDLAlterRoutine::getAlterPassThroughParamArray() const {
  return alterPassThroughParamArray_;
}

inline const ElemDDLPassThroughParamDefArray &StmtDDLAlterRoutine::getAddPassThroughParamArray() const {
  return getPassThroughParamArray();
}

// methods relating to parse trees and nodes

inline ElemDDLNode *StmtDDLAlterRoutine::getAddPassThroughInputsParseTree() { return getPassThroughInputsParseTree(); }

inline ElemDDLNode *StmtDDLAlterRoutine::getAlterPassThroughInputsParseTree() {
  return alterPassThroughInputsParseTree_;
}

inline ElemDDLPassThroughParamDefArray &StmtDDLAlterRoutine::getAddPassThroughParamArray() {
  return getPassThroughParamArray();
}

//
// mutators
//

inline ElemDDLPassThroughParamDefArray &StmtDDLAlterRoutine::getAlterPassThroughParamArray() {
  return alterPassThroughParamArray_;
}

// methods relating to parse trees and nodes

inline void StmtDDLAlterRoutine::setAddPassThroughInputsParseTree(ElemDDLNode *pParseTree) {
  setPassThroughInputsParseTree(pParseTree);
}

inline void StmtDDLAlterRoutine::setAlterPassThroughInputsParseTree(ElemDDLNode *pParseTree) {
  alterPassThroughInputsParseTree_ = pParseTree;
}

#endif  // STMTDDLALTERROUTINE_H
