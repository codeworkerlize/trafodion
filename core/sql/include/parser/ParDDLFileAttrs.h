
#ifndef PARDDLFILEATTRS_H
#define PARDDLFILEATTRS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ParDDLFileAttrs.h
 * Description:  base class for derived classes to contain all the file
 *               attribute information associating with the parse node
 *               representing a DDL statement (for example, create table
 *               statement)
 *
 *
 * Created:      5/25/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "export/NABasicObject.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ParDDLFileAttrs;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class ParDDLFileAttrs
// -----------------------------------------------------------------------
class ParDDLFileAttrs : public NABasicObject {
 public:
  // types of nodes containing all legal file attributes
  // associating with a DDL statement
  enum fileAttrsNodeTypeEnum {
    FILE_ATTRS_ANY_DDL_STMT,
    FILE_ATTRS_ALTER_INDEX,
    FILE_ATTRS_ALTER_TABLE,
    FILE_ATTRS_ALTER_VIEW,
    FILE_ATTRS_CREATE_INDEX,
    FILE_ATTRS_CREATE_TABLE
  };

  // default constructor
  ParDDLFileAttrs(fileAttrsNodeTypeEnum fileAttrsNodeType = FILE_ATTRS_ANY_DDL_STMT)
      : fileAttrsNodeType_(fileAttrsNodeType) {}

  // virtual destructor
  virtual ~ParDDLFileAttrs();

  // accessor
  inline fileAttrsNodeTypeEnum getFileAttrsNodeType() const;

  // mutator
  void copy(const ParDDLFileAttrs &rhs);

 private:
  fileAttrsNodeTypeEnum fileAttrsNodeType_;

};  // class ParDDLFileAttrs

// -----------------------------------------------------------------------
// definitions of inline methods for class ParDDLFileAttrs
// -----------------------------------------------------------------------

// accessor
ParDDLFileAttrs::fileAttrsNodeTypeEnum ParDDLFileAttrs::getFileAttrsNodeType() const { return fileAttrsNodeType_; }

#endif  // PARDDLFILEATTRS_H
