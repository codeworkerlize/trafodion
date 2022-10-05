
#ifndef ELEMDDL_MV_FILEATTR_COMMIT_EACH_H
#define ELEMDDL_MV_FILEATTR_COMMIT_EACH_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLFileAttrMVCommitEach.h
* Description:  class for MVS COMMIT EACH nrows File Attribute (parse node)
*               elements in DDL statements
*
*
* Created:      04/02/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLFileAttr.h"

class ElemDDLFileAttrMVCommitEach : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrMVCommitEach(int nrows = 0) : ElemDDLFileAttr(ELM_FILE_ATTR_MV_COMMIT_EACH_ELEM), nrows_(nrows) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrMVCommitEach();

  // cast
  virtual ElemDDLFileAttrMVCommitEach *castToElemDDLFileAttrMVCommitEach();

  int getNRows() const { return nrows_; }

  // member functions for tracing
  virtual const NAString getText() const;
  virtual const NAString displayLabel1() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  int nrows_;

};  // class ElemDDLFileAttrMVCommitEach

#endif  // ELEMDDL_MV_FILEATTR_COMMIT_EACH_H
