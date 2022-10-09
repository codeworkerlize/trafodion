
#ifndef ELEMDDLQUALNAME_H
#define ELEMDDLQUALNAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLQualName.h
 * Description:  an element representing a qualified name
 *					used as a node in a list of qualified names
 *
 * Created:      06/20/99
 * Language:     C++
 * Project:
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLNode.h"
//#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "optimizer/ObjectNames.h"

class ElemDDLQualName;

class ElemDDLQualName : public ElemDDLNode {
 public:
  ElemDDLQualName(const QualifiedName &qualName);

  virtual ~ElemDDLQualName();

  virtual ElemDDLQualName *castToElemDDLQualName();

  inline const NAString getName() const;
  inline const QualifiedName &getQualifiedName() const;
  inline QualifiedName &getQualifiedName();

 private:
  QualifiedName qualName_;

};  // class ElemDDLQualName

//----------------------------------------------------------------------------
inline const NAString ElemDDLQualName::getName() const { return qualName_.getQualifiedNameAsAnsiString(); }

inline QualifiedName &ElemDDLQualName::getQualifiedName() { return qualName_; }

inline const QualifiedName &ElemDDLQualName::getQualifiedName() const { return qualName_; }

#endif  // ELEMDDLQUALNAME_H
