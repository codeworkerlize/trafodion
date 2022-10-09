
#ifndef ELEMDDLGRANTEE_H
#define ELEMDDLGRANTEE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLGrantee.h
 * Description:  class for Grantee (parse node) elements in Grant
 *               DDL statements
 *
 *
 * Created:      10/16/95
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
class ElemDDLGrantee;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Column Name (parse node) elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLGrantee : public ElemDDLNode {
 public:
  // constructors
  ElemDDLGrantee(CollHeap *h = 0) : ElemDDLNode(ELM_GRANTEE_ELEM), isPublic_(TRUE), authorizationIdentifier_(h) {}

  ElemDDLGrantee(const NAString &authorizationIdentifier, CollHeap *h = 0)
      : ElemDDLNode(ELM_GRANTEE_ELEM), isPublic_(FALSE), authorizationIdentifier_(authorizationIdentifier, h) {}

  // copy ctor
  ElemDDLGrantee(const ElemDDLGrantee &orig, CollHeap *h = 0);  // not written

  // virtual destructor
  virtual ~ElemDDLGrantee();

  // cast
  virtual ElemDDLGrantee *castToElemDDLGrantee();

  //
  // accessors
  //

  inline const NAString &getAuthorizationIdentifier() const;

  // returns the authorization identifier of the grantee
  // if the grantee is not PUBLIC; otherwise, returns
  // an empty string (i.e., the returned value has no
  // meaning).

  inline NABoolean isPublic() const;

  // returns TRUE if grantee is PUBLIC;
  // returns FALSE otherwise.

  // member functions for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NABoolean isPublic_;
  NAString authorizationIdentifier_;

};  // class ElemDDLGrantee

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLGrantee
// -----------------------------------------------------------------------
//
// accessors
//

inline const NAString &ElemDDLGrantee::getAuthorizationIdentifier() const { return authorizationIdentifier_; }

inline NABoolean ElemDDLGrantee::isPublic() const { return isPublic_; }

#endif  // ELEMDDLGRANTEE_H
