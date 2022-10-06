
#ifndef ELEMDDLTENANTRGROUP_H
#define ELEMDDLTENANTRGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLTenantResourceGroup.h
 * Description:  Description of a resource group for a tenant
 *
 * Language:     C++
 *
 *****************************************************************************
 */

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "optimizer/ObjectNames.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLTenantResourceGroup;

// -----------------------------------------------------------------------
// ElemDDLTenantResourceGroup
//
// A temporary parse node to contain a group name and an optional
// configuration section
// -----------------------------------------------------------------------
class ElemDDLTenantResourceGroup : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLTenantResourceGroup(const NAString &groupName, ConstStringList *nodeList, CollHeap *heap = PARSERHEAP())
      : ElemDDLNode(ELM_TENANT_RGROUP_ELEM), groupName_(groupName, heap), nodeList_(nodeList) {}

  // copy ctor
  ElemDDLTenantResourceGroup(const ElemDDLTenantResourceGroup &orig, CollHeap *heap = PARSERHEAP());  // not written

  // virtual destructor
  virtual ~ElemDDLTenantResourceGroup() {}

  // cast
  virtual ElemDDLTenantResourceGroup *castToElemDDLTenantResourceGroup() { return this; };

  // accessors

  const NAString getGroupName() const { return groupName_; }
  const ConstStringList *getNodeList() const { return nodeList_; }

 private:
  NAString groupName_;
  ConstStringList *nodeList_;

};  // class ElemDDLTenantResourceGroup

#endif /* ELEMDDLTENANTRGROUP_H */
