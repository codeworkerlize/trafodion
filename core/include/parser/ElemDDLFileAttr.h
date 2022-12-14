
#ifndef ELEMDDLFILEATTR_H
#define ELEMDDLFILEATTR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLFileAttr.h
 * Description:  base class for all File Attribute parse nodes in DDL
 *               statements
 *
 *
 * Created:      5/30/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComASSERT.h"
#include "common/ComSmallDefs.h"
#include "common/ComUnits.h"
#include "parser/ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttr;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// base class for all File Attribute parse nodes in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttr : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLFileAttr(OperatorTypeEnum operType = ELM_ANY_FILE_ATTR_ELEM) : ElemDDLNode(operType) {}

  // virtual destructor
  virtual ~ElemDDLFileAttr();

  // cast
  virtual ElemDDLFileAttr *castToElemDDLFileAttr();

  // given a size unit enumerated constant,
  // return an appropriate NAString.
  NAString convertSizeUnitToNAString(ComUnits sizeUnit) const;

  // methods for tracing
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const {
    ComASSERT(FALSE);
    return "";
  }

 private:
};  // class ElemDDLFileAttr

// class ElemDDLFileAttrCompression
class ElemDDLFileAttrCompression : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrCompression(ComCompressionType compressionSpec)
      : ElemDDLFileAttr(ELM_FILE_ATTR_COMPRESSION_ELEM), compressionType_(compressionSpec) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrCompression();

  // cast
  virtual ElemDDLFileAttrCompression *castToElemDDLFileAttrCompression();

  // accessor
  const ComCompressionType getCompressionType() const { return compressionType_; }

 private:
  ComCompressionType compressionType_;

};  // class ElemDDLFileAttrCompression

class ElemDDLFileAttrUID : public ElemDDLFileAttr {
 public:
  ElemDDLFileAttrUID(long UID);

  // virtual destructor
  virtual ~ElemDDLFileAttrUID();

  // cast
  virtual ElemDDLFileAttrUID *castToElemDDLFileAttrUID();

  // accessors
  inline long getUID() const { return UID_; }

 private:
  long UID_;

};  // class ElemDDLFileAttrUID

class ElemDDLFileAttrRowFormat : public ElemDDLFileAttr {
 public:
  enum ERowFormat { eUNSPECIFIED, ePACKED, eALIGNED, eHBASE };

 public:
  ElemDDLFileAttrRowFormat(ERowFormat rowFormat);

  // virtual destructor
  virtual ~ElemDDLFileAttrRowFormat();

  // cast
  virtual ElemDDLFileAttrRowFormat *castToElemDDLFileAttrRowFormat();

  // accessors
  inline ERowFormat getRowFormat() const { return eRowFormat_; }

  // method for building text
  virtual NAString getSyntax() const;

 private:
  ERowFormat eRowFormat_;

};  // class ElemDDLFileAttrRowFormat

class ElemDDLFileAttrColFam : public ElemDDLFileAttr {
 public:
  ElemDDLFileAttrColFam(NAString &colFam) : ElemDDLFileAttr(ELM_FILE_ATTR_COL_FAM_ELEM), colFam_(colFam) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrColFam(){};

  // cast
  virtual ElemDDLFileAttrColFam *castToElemDDLFileAttrColFam() { return this; }

  // accessors
  NAString &getColFam() { return colFam_; }

  // method for building text
  virtual NAString getSyntax() const { return ""; }

 private:
  NAString colFam_;

};  // class ElemDDLFileAttrColFam

class ElemDDLFileAttrXnRepl : public ElemDDLFileAttr {
 public:
  ElemDDLFileAttrXnRepl(ComReplType xnRepl) : ElemDDLFileAttr(ELM_FILE_ATTR_XN_REPL_ELEM) { xnRepl_ = xnRepl; }

  // virtual destructor
  virtual ~ElemDDLFileAttrXnRepl(){};

  // cast
  virtual ElemDDLFileAttrXnRepl *castToElemDDLFileAttrXnRepl() { return this; }

  ComReplType xnRepl() { return xnRepl_; }

  // method for building text
  virtual NAString getSyntax() const { return ""; }

 private:
  ComReplType xnRepl_;
};  // class ElemDDLFileAttrXnRepl

class ElemDDLFileAttrStorageType : public ElemDDLFileAttr {
 public:
  ElemDDLFileAttrStorageType(ComStorageType storageType) : ElemDDLFileAttr(ELM_FILE_ATTR_STORAGE_TYPE_ELEM) {
    storageType_ = storageType;
  }

  // virtual destructor
  virtual ~ElemDDLFileAttrStorageType(){};

  // cast
  virtual ElemDDLFileAttrStorageType *castToElemDDLFileAttrStorageType() { return this; }

  ComStorageType storageType() { return storageType_; }

  // method for building text
  virtual NAString getSyntax() const { return ""; }

 private:
  ComStorageType storageType_;
};  // class ElemDDLFileAttrStorageType

class ElemDDLFileAttrStoredDesc : public ElemDDLFileAttr {
 public:
  ElemDDLFileAttrStoredDesc() : ElemDDLFileAttr(ELM_FILE_ATTR_STORED_DESC_ELEM) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrStoredDesc(){};

  // cast
  virtual ElemDDLFileAttrStoredDesc *castToElemDDLFileAttrStoredDesc() { return this; }

  // method for building text
  virtual NAString getSyntax() const { return ""; }

 private:
};  // class ElemDDLFileAttrStoredDesc

#endif  // ELEMDDLFILEATTR_H
