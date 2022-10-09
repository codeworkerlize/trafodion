
#ifndef ELEMDDLFILEATTRLOCKLENGTH_H
#define ELEMDDLFILEATTRLOCKLENGTH_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrLockLength.h
* Description:  class for Generic Lock Length (parse node) elements
*               in DDL statements
*
*
* Created:      4/21/95
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "parser/ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrLockLength;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Generic Lock Length (parse node) elements in DDL statements
// -----------------------------------------------------------------------
class ElemDDLFileAttrLockLength : public ElemDDLFileAttr {
 public:
  // constructor
  ElemDDLFileAttrLockLength(unsigned short lockLengthInBytes)
      : ElemDDLFileAttr(ELM_FILE_ATTR_LOCK_LENGTH_ELEM), lockLengthInBytes_(lockLengthInBytes) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrLockLength();

  // cast
  virtual ElemDDLFileAttrLockLength *castToElemDDLFileAttrLockLength();

  // accessor
  inline unsigned short getLockLength() const;

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

 private:
  unsigned short lockLengthInBytes_;

};  // class ElemDDLFileAttrLockLength

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLFileAttrLockLength
// -----------------------------------------------------------------------
//
// accessor
//

inline unsigned short ElemDDLFileAttrLockLength::getLockLength() const { return lockLengthInBytes_; }

#endif  // ELEMDDLFILEATTRLOCKLENGTH_H
