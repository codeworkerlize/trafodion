
#ifndef ELEMDDLINDEXSCOPEOPTION_H
#define ELEMDDLINDEXSCOPEOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLIndexScopeOption.h
 * Description:  class for Create Index LOCAL OR GLOBAL
 *
 *
 * Created:      04/01/21
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"

class ElemDDLIndexScopeOption : public ElemDDLNode {
 public:
  ElemDDLIndexScopeOption() : ElemDDLNode(ELM_WITH_SCOPE_OPTION_ELEM) {
    LOCAL_OPT_ = FALSE;
    GLOBAL_OPT_ = FALSE;
  }

  // return 1 if populate is specified. Otherwise, return 0.
  ComBoolean getIndexLocalOption() { return LOCAL_OPT_; };

  // return 1 if no populate is specified . Otherwise, return 0.
  ComBoolean getIndexGlobalOption() { return GLOBAL_OPT_; };

  // specified the no populate clause.
  void setIndexLocalClause(ComBoolean option) { LOCAL_OPT_ = option; };

  // specified the populate clause.
  void setIndexGlobalClause(ComBoolean option) { GLOBAL_OPT_ = option; };

  // cast virtual function.
  virtual ElemDDLIndexScopeOption *castToElemDDLIndexScopeOption() { return this; };

 private:
  ComBoolean LOCAL_OPT_;
  ComBoolean GLOBAL_OPT_;
};

#endif  // ELEMDDLINDEXSCOPEOPTION_H