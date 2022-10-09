
#ifndef ELEMDDLGROUP_H
#define ELEMDDLGROUP_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLGroup.h
 * Description:  class for Group infomation elements in ALTER GROUP
 *               DDL statements
 *
 *
 * Created:      02/26/2020
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------

class ElemDDLGroup : public ElemDDLNode {
 public:
  // constructors
  ElemDDLGroup(NAString &authorizationIdentifier, CollHeap *h = 0)
      : ElemDDLNode(ELM_GROUP_ELEM), authorizationIdentifier_(authorizationIdentifier, h) {}

  // copy ctor
  ElemDDLGroup(const ElemDDLGroup &orig, CollHeap *h = 0);  // not written

  virtual ~ElemDDLGroup(){};

  // cast
  virtual ElemDDLGroup *castToElemDDLGroup() { return this; };

  //
  // accessors
  //

  inline const NAString &getAuthorizationIdentifier() const { return authorizationIdentifier_; };

  // member functions for tracing
  virtual const NAString displayLabel1() const {
    return (NAString("Authorization identifier: ") + this->getAuthorizationIdentifier());
  }
  virtual const NAString getText() const { return "ElemDDLGroup"; }

 private:
  NAString authorizationIdentifier_;

};  // class ElemDDLGroup

#endif  // ELEMDDLGROUP_H