
#ifndef ELEMDDLTENANTGROUP_H
#define ELEMDDLTENANTGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTenantSchema.h
 * Description:  Description of a tenant schema
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "optimizer/ObjectNames.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLTenantGroup;

// -----------------------------------------------------------------------
// ElemDDLTenantGroup
//
// A temporary parse node to contain a group name and an optional
// configuration section
// -----------------------------------------------------------------------
class ElemDDLTenantGroup : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLTenantGroup(const NAString &groupName, const char *pConfig, CollHeap *heap = PARSERHEAP())
      : ElemDDLNode(ELM_TENANT_GROUP_ELEM), groupName_(groupName, heap) {
    if (pConfig == NULL)
      config_ = NAString("", heap);
    else
      config_ = NAString(pConfig, heap);
  }

  // copy ctor
  ElemDDLTenantGroup(const ElemDDLTenantGroup &orig, CollHeap *heap = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLTenantGroup() {}

  // cast
  virtual ElemDDLTenantGroup *castToElemDDLTenantGroup() { return this; };

  // accessors

  const NAString getGroupName() const { return groupName_; }
  const NAString getConfig() const { return config_; }

 private:
  NAString groupName_;
  NAString config_;

};  // class ElemDDLTenantGroup

#if 0
class ElemDDLTenantGroupList : public ElemDDLList
{

public:
  
  // default constructor
  ElemDDLTenantGroupList(ElemDDLNode * commaExpr, ElemDDLNode * otherExpr)
  : ElemDDLList(ELM_TENANT_GROUP_ELEM, commaExpr, otherExpr) {}
  
  // virtual destructor
  virtual ~ElemDDLTenantGroupList() {};

  virtual ElemDDLTenantGroupList * castToElemDDLTenantGroupList() { return this; }
private:

}; // class ElemDDLTenantGroupList
#endif

#endif /* ELEMDDLTENANTGROUP_H */
