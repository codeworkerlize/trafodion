/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
#ifndef EXP_ATTRS_H
#define EXP_ATTRS_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         exp_attrs.h
 * Description:  Data type information for the run-time components.
 *
 *
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/dfs2rec.h"
#include "common/str.h"
#include "common/OperTypeEnum.h"
#include "common/NAAssert.h"
#include "export/NAVersionedObject.h"
#include "exp/ExpError.h"
#include "exp/exp_tuple_desc.h"
#include "common/charinfo.h"

#define ALIGN(offset, alignment) ((offset > 0) ? (((offset - 1) / alignment) + 1) * alignment : offset)

#define MAX_COLNAME_LEN_DISPLAY 100

class Attributes;
class SimpleType;
class ComplexType;

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class Attributes;
class SimpleType;
class ComplexType;
class ShowPlanAttributes;

// -----------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------
class ExpDatetime;

// Max size for any of the attribute offset.  Typically used to designate
// that the size is uninitialized and should not be used and some other
// offset value should be used instead.
// The offset for all variable length fields except the first variable field
// will be set to this value.  The offset to the field must be read from its
// voa offset.
// If the voa offset is set to this value, then on insert or update there is
// no voa offset that needs to be changed.
// The voa offset is set this value for all fixed fields, except the first
// fixed field.  Variable length fields have a real value in their voa offset.
// static const UInt32 ExpOffsetMax = UINT_MAX;

// Size of each VOA array entry for SQLMX_FORMAT and SQLMX_ALIGNED_FORMAT
// static const UInt32 ExpVoaSize = sizeof(int);

class Attributes : public NAVersionedObject {
 public:
  // Possible types of Attributes
  // (these codes must remain invariant across future versions)
  enum AttrClassID {
    AttribAnchorID = -1,
    ComplexTypeID = 2,
    SimpleTypeID = 3,
    ShowplanID = 4,
    BigNumID = 5,
    CompositeAttributesID = 6,
  };

  enum DefaultClass {
    NO_DEFAULT,
    DEFAULT_NULL,
    DEFAULT_CURRENT,
    DEFAULT_USER,
    DEFAULT_USER_FUNCTION,
    DEFAULT_IDENTITY,
    DEFAULT_CURRENT_UT,
    DEFAULT_UUID,
    DEFAULT_FUNCTION,
    DEFAULT_CATALOG_FUNCTION,
    DEFAULT_SCHEMA_FUNCTION,
    DEFAULT_SYS_GUID_FUNCTION,
    INVALID_DEFAULT
  };

  enum AttrRowsetEnum {
    ATR_USE_TOTAL_ROWSET_SIZE = 0x0001,      // Indicates to use whole
                                             // rowset size in DP2
    ATR_HV_ROWSET_FOR_INPUT_SIZE = 0x0002,   // This is the host var in
                                             // ROWSET FOR INPUT SIZE <var>
    ATR_HV_ROWSET_FOR_OUTPUT_SIZE = 0x0004,  // This is the host var in
                                             // ROWSET FOR OUTPUT SIZE <var>
    ATR_HV_ROWSET_LOCAL_SIZE = 0x0008,       // This is the host var in
                                             // ROWSET <var> ( <list >)
    ATR_INPUT_ASSIGNMENT = 0x0010,
    ATR_HV_ROWWISE_ROWSET_INPUT_BUFFER = 0x0020,  // this hostvar/param
                                                  // contains the address of
                                                  // rowwise rowset input buffer
    ATR_HV_BLANK = 0x0040,                        // Dummy host var
    ATR_NOT_A_FLAG = 0x8000                       // We put other information
                                                  // that is not a flag
  };

  // isDefaultValueNeeded
  //   Static method to determine if a column value is missing or not from
  //   a data row.
  //   This is only for SQLMX_FORMAT and SQLMX_ALIGNED_FORMAT.
  static NABoolean isDefaultValueNeeded(Attributes *attr, ExpTupleDesc *tuppDesc, UInt32 firstFixedOffset,
                                        UInt16 varIndLength, UInt32 voaOffset, char *dataPtr, UInt32 dataLen) {
    NABoolean rtnStatus = FALSE;

    if (attr->isAddedCol()) {
      UInt32 firstVarOffset = tuppDesc->getFirstVariableOffset(dataPtr, firstFixedOffset);
      UInt32 numFillerBytes = tuppDesc->getNumberFillerBytes(dataPtr);
      NABoolean isVarField = (varIndLength > 0);
      UInt32 bitmapOffset = (attr->isSQLMXAlignedFormat() ? ExpAlignedFormat::getBitmapOffset(dataPtr) : ExpOffsetMax);

      // Check if this is an added column that requires
      // a default value.  Must be marked at compile
      // time that it is "special" (ie. added)
      // AND one of the following:
      // 1. variable field and voaOffset >= bitmap offset
      // 2. first variable field
      // 3. variable field and voaOffset >= first variable
      //    field offset with no fixed fields in table.
      // 4. variable field and voaOffset >= first fixed
      //    field offset (fixed and variable fields)
      // 5. variable field and value at voaOffset is 0
      //    (padding)
      // 6. fixed field, but no fixed fields in record
      // 7. fixed field and first fixed offset and
      //    relative offset > dataLen
      // 8. fixed field and first fixed offset plus
      //    relative offset > dataLen
      //    (fixed and variable fields in record)
      rtnStatus =
          ((isVarField && (bitmapOffset > 0) && (voaOffset >= bitmapOffset)) || (isVarField && (firstVarOffset == 0)) ||
           (isVarField && (firstFixedOffset == 0) && (voaOffset >= firstVarOffset)) ||
           (isVarField && (firstFixedOffset > 0) && (voaOffset >= firstFixedOffset)) ||
           (isVarField && (tuppDesc->getVoaOffset(dataPtr, voaOffset) == 0)) ||
           ((!isVarField) && (firstFixedOffset == 0)) ||
           ((!isVarField) && (firstFixedOffset + attr->getRelOffset()) >= (dataLen - numFillerBytes)) ||
           ((!isVarField) && (firstVarOffset > 0) && (firstFixedOffset + attr->getRelOffset()) >= firstVarOffset));
    }

    return rtnStatus;
  }

  Attributes(Int16 complex_type = 0);

  ~Attributes() {}

  void setDatatype(Int16 datatype) {
    datatype_ = datatype;
    if (datatype == REC_NUM_BIG_SIGNED || datatype == REC_NUM_BIG_UNSIGNED) setClassID(BigNumID);
  }

  Int16 getDatatype() { return datatype_; }

  void setNullFlag(Int16 nullFlag) { nullFlag_ = nullFlag; }
  Int16 getNullFlag() { return nullFlag_; }

  void setNullIndicatorLength(Int16 len) { nullIndicatorLength_ = len; }
  Int16 getNullIndicatorLength() { return nullIndicatorLength_; }

  void setVCIndicatorLength(Int16 len) { vcIndicatorLength_ = len; }
  Int16 getVCIndicatorLength() { return vcIndicatorLength_; }

  NABoolean isVariableLength() { return (getVCIndicatorLength() > 0); }

  void setAtp(Int16 atp) { atp_ = atp; }
  Int16 getAtp() { return atp_; }

  void setAtpIndex(Int16 atpindex) { atpindex_ = atpindex; }
  Int16 getAtpIndex() { return atpindex_; }

  void setOffset(UInt32 offset) { offset_ = offset; };
  UInt32 getOffset() { return offset_; }

  void setNullIndOffset(UInt32 o) { nullIndOffset_ = o; };
  int getNullIndOffset() { return nullIndOffset_; }

  void setNullBitIndex(UInt32 bit) { nullBitIdx_ = (Int16)bit; };
  Int16 getNullBitIndex() { return nullBitIdx_; }

  void setVCLenIndOffset(UInt32 o) { vcLenIndOffset_ = o; };
  int getVCLenIndOffset() { return vcLenIndOffset_; }

  void setVoaOffset(UInt32 i) { voaOffset_ = i; }
  UInt32 getVoaOffset() { return voaOffset_; }

  void setRelOffset(UInt32 i) { relOffset_ = i; }
  UInt32 getRelOffset() { return relOffset_; }

  int getRowsetSize() { return rowsetSize_; };
  void setRowsetSize(int rowset_size) { rowsetSize_ = rowset_size; };

  UInt32 getValId() { return valId_; }
  void setValId(UInt32 valId) { valId_ = valId; }

  char *getColName() { return colName_; }
  void setColName(const char *colName) {
    if (colName) {
      // if column name more than 100 then cut off at 100
      int len = MINOF(str_len(colName), MAX_COLNAME_LEN_DISPLAY);
      str_cpy(colName_, colName, len, '\0');
      colName_[len] = 0;
    } else
      colName_[0] = 0;
  }

  NABoolean getUseTotalRowsetSize() { return ((rowsetInfo_ & ATR_USE_TOTAL_ROWSET_SIZE) != 0); };

  void resetUseTotalRowsetSize() { rowsetInfo_ &= ~ATR_USE_TOTAL_ROWSET_SIZE; };

  void setUseTotalRowsetSize() { rowsetInfo_ |= ATR_USE_TOTAL_ROWSET_SIZE; };

  void setBlankHV() { rowsetInfo_ |= ATR_HV_BLANK; };

  NABoolean getHVRowsetForInputSize() { return ((rowsetInfo_ & ATR_HV_ROWSET_FOR_INPUT_SIZE) != 0); };

  NABoolean getHVRowsetForOutputSize() { return ((rowsetInfo_ & ATR_HV_ROWSET_FOR_OUTPUT_SIZE) != 0); };

  NABoolean getHVRowsetLocalSize() { return ((rowsetInfo_ & ATR_HV_ROWSET_LOCAL_SIZE) != 0); };

  NABoolean getHVRowwiseRowsetInputBuffer() { return ((rowsetInfo_ & ATR_HV_ROWWISE_ROWSET_INPUT_BUFFER) != 0); };

  NABoolean isBlankHV() { return ((rowsetInfo_ & ATR_HV_BLANK) != 0); };

  Int16 getRowsetInfo() { return rowsetInfo_; };

  void setRowsetInfo(Int16 info) { rowsetInfo_ = info; };

  void setTupleFormat(ExpTupleDesc::TupleDataFormat tdf) { tdf_ = tdf; }

  ExpTupleDesc::TupleDataFormat getTupleFormat() { return (ExpTupleDesc::TupleDataFormat)tdf_; }

  NABoolean isSQLMXFormat() { return (tdf_ == ExpTupleDesc::SQLMX_FORMAT); }

  NABoolean isSQLMXAlignedFormat() { return (tdf_ == ExpTupleDesc::SQLMX_ALIGNED_FORMAT); }

  NABoolean isSQLPackedFormat() { return (tdf_ == ExpTupleDesc::PACKED_FORMAT); }

  NABoolean isSQLMXDiskFormat() {
    return ((tdf_ == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) || (tdf_ == ExpTupleDesc::SQLMX_FORMAT));
  }

  NABoolean isTrafodionDiskFormat() {
    return ((tdf_ == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) || (tdf_ == ExpTupleDesc::SQLARK_EXPLODED_FORMAT));
  }

  DefaultClass getDefaultClass() { return (DefaultClass)defClass_; }

  void setDefaultClass(DefaultClass dc) { defClass_ = dc; }

  char *getDefaultValue() { return defaultValue_; }

  void setDefaultValue(DefaultClass dc, char *dv) {
    defClass_ = dc;
    defaultValue_ = dv;
  }

  UInt32 getDefaultFieldNum() { return defaultFieldNum_; }
  void setDefaultFieldNum(UInt32 fn) { defaultFieldNum_ = fn; }

  // set whether item is always aligned properly
  void needDataAlignment();
  void dontNeedDataAlignment();

  // is this data item always aligned correctly?
  int isNotAlwaysAligned();
  // what is the alignment of this item (1, 2, 4, 8 byte alignment)
  int getDataAlignmentSize() { return alignment_; }
  void setDataAlignmentSize(int a) { alignment_ = (Int16)a; }

  int isComplexType() { return flags_ & COMPLEX_TYPE; }
  Int16 isSimpleType() { return !(flags_ & COMPLEX_TYPE); }

  NABoolean isAddedCol() { return (flags_ & ADDED_COL) != 0; }
  void setAddedCol() { flags_ |= ADDED_COL; }

  void setShowplan() { flags_ |= SHOWPLAN_; }
  void resetShowplan() { flags_ &= ~SHOWPLAN_; }
  NABoolean showplan() { return (flags_ & SHOWPLAN_) != 0; }

  void setUpshift(Int16 v) { (v ? flags_ |= UPSHIFT_ : flags_ &= ~UPSHIFT_); }
  NABoolean upshift() { return (flags_ & UPSHIFT_) != 0; }

  void setCaseinsensitive(Int16 v) { (v ? flags_ |= CASEINSENSITIVE_ : flags_ &= ~CASEINSENSITIVE_); }
  NABoolean isCaseinsensitive() { return (flags_ & CASEINSENSITIVE_) != 0; }

  void setWidechar(Int16 v) { (v ? flags_ |= WIDECHAR_ : flags_ &= ~WIDECHAR_); }
  NABoolean widechar() { return (flags_ & WIDECHAR_) != 0; }

  NABoolean isGuOutput() { return (flags_ & GU_OUTPUT) != 0; }
  void setGuOutput() { flags_ |= GU_OUTPUT; }

  // Should this field be treated as a fixed value.  Relevant for
  // VARCHARS in aligned row format.  Used for VarChar aggregates by HashGroupby.
  NABoolean isForceFixed() { return (flags_ & FORCE_FIXED_) != 0; }
  void setForceFixed() { flags_ |= FORCE_FIXED_; }

  NABoolean isLengthInKB() { return (flags_ & LENGTH_IN_KB_) != 0; }
  void setLengthInKB() { flags_ |= LENGTH_IN_KB_; }

  NABoolean isHbaseMapTableCol() { return (flags_ & IS_HBASE_MAPTABLE_COL) != 0; }
  void setIsHbaseMapTableCol() { flags_ |= IS_HBASE_MAPTABLE_COL; }

  NABoolean isSpaceSensitive() { return (flags_ & SPACE_SENSITIVE_) != 0; }
  void setSpaceSensitive() { flags_ |= SPACE_SENSITIVE_; }

  // Bulk move flags
  void setBulkMoveable(NABoolean flag = TRUE) { (flag ? flags_ |= BULK_MOVE_ : flags_ &= ~BULK_MOVE_); }
  NABoolean isBulkMoveable() { return (flags_ & BULK_MOVE_) != 0; }

  // The following methods are used to turn on/off the flags for new
  // last day of month processing for datetime intervals.
  void setlastdaymonthflag() { flags_ |= LAST_DAY_MONTH; }
  NABoolean getlastdaymonthflag() { return (flags_ & LAST_DAY_MONTH) != 0; }
  void resetlastdaymonthflag() { flags_ &= ~LAST_DAY_MONTH; }

  void setlastdayonerrflag() { flags_ |= LAST_DAY_ERROR; }
  NABoolean getlastdayonerrflag() { return (flags_ & LAST_DAY_ERROR) != 0; }
  void resetlastdayonerrflag() { flags_ &= ~LAST_DAY_ERROR; }

  // These next two methods are needed to enable re-assembling a row of
  // attribute values from a set of fragments (an abort after an update).
  void setNextFieldIndex(CollIndex i) { nextAttrIdx_ = i; }
  UInt32 getNextFieldIndex() { return nextAttrIdx_; }

  NABoolean isComposite() { return DFS2REC::isComposite(getDatatype()); }

  // Indirect varchars are the ones beyond the first varchar. They have offset set to -1.
  NABoolean isIndirectVC() { return ((getOffset() == UINT_MAX) && (getVCIndicatorLength() > 0)); }

  // return TRUE if this attr is a NUMERIC datatype.
  NABoolean isNumericWithPrecision() {
    if (((DFS2REC::isBinaryNumeric(getDatatype())) || (DFS2REC::isBigNum(getDatatype()))) && (getPrecision() > 0))
      return TRUE;
    else
      return FALSE;
  }

  ////////////////////////////////////////////////////////
  // returns the actual length from the input data,
  // if a variable length field. Otherwise, calls the
  // virtual function getLength().
  ///////////////////////////////////////////////////////
  UInt32 getLength(const char *data) {
    if (getVCIndicatorLength() > 0) {
      char temp[8];
      str_cpy_all(temp, data, getVCIndicatorLength());
      if (getVCIndicatorLength() == sizeof(Int16))
        return *(UInt16 *)temp;
      else
        return *(UInt32 *)temp;
    } else
      return getLength();
  }

  void setVarLength(UInt32 length, char *data) {
    if (getVCIndicatorLength() > 0) {
      // Check that len0 is not greater than the implementation-defined max
      // length (MAXVARCHAR) for variable-length strings.
      // If len0 > MAXVARCHAR, and if (MAXVARCHAR - len0) characters are all
      // spaces, make len0 = MAXVARCHAR. Otherwise, raise a SQL-exception.
      // TBD.

      if (getVCIndicatorLength() == sizeof(Int16)) {
        assert(length <= USHRT_MAX);
        UInt16 temp = (UInt16)length;
        str_cpy_all(data, (char *)&temp, sizeof(Int16));
      } else
        str_cpy_all(data, (char *)&length, getVCIndicatorLength());
    }
  }

  void copyLocationAttrs(Attributes *attr) {
    atp_ = attr->getAtp();
    atpindex_ = attr->getAtpIndex();
    offset_ = attr->getOffset();
    nullIndOffset_ = attr->getNullIndOffset();
    vcLenIndOffset_ = attr->getVCLenIndOffset();
    voaOffset_ = attr->getVoaOffset();
    relOffset_ = attr->getRelOffset();
    tdf_ = attr->getTupleFormat();
    rowsetSize_ = attr->getRowsetSize();
    rowsetInfo_ = attr->getRowsetInfo();
    nullBitIdx_ = attr->getNullBitIndex();

    defaultFieldNum_ = attr->getDefaultFieldNum();

    vcIndicatorLength_ = attr->getVCIndicatorLength();
    nullIndicatorLength_ = attr->getNullIndicatorLength();

    nextAttrIdx_ = attr->getNextFieldIndex();

    // if alignment is needed, copy that
    if (attr->isNotAlwaysAligned()) needDataAlignment();

    if (attr->isAddedCol() && !isHbaseMapTableCol()) setAddedCol();

    if (attr->isBulkMoveable())
      setBulkMoveable(TRUE);
    else
      setBulkMoveable(FALSE);

    if (attr->isSpaceSensitive()) setSpaceSensitive();
  };

  // VIRTUAL functions
  virtual int getStorageLength();
  virtual int getDefaultValueStorageLength();
  virtual int getLength();
  virtual Attributes *newCopy();
  virtual Attributes *newCopy(CollHeap *);
  virtual void copyAttrs(Attributes *source_);

  virtual Int16 getScale() { return -1; }
  virtual UInt16 getScaleAsUI() { return 0; }
  virtual int getPrecision() { return -1; }
  virtual CharInfo::Collation getCollation() { return CharInfo::UNKNOWN_COLLATION; }

  virtual CharInfo::CharSet getCharSet() { return CharInfo::UnknownCharSet; }

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  virtual void fixup(Space *space, char *constants_area, char *temps_area, char *persistent_area,
                     Int16 fixupConstsAndTemps = 0, NABoolean spaceCompOnly = FALSE);

  virtual void displayContents(ostream &out, Attributes *spAttrs = NULL);

  virtual void displayContents(Space *space, int operandNum, char *constsArea, Attributes *spAttrs);

  // ---------------------------------------------------------------------
  // Perform type-safe pointer casts.
  // ---------------------------------------------------------------------
  virtual ExpDatetime *castToExpDatetime();

  // ---------------------------------------------------------------------
  // Method for comparing if two Attributes are equal.
  // ---------------------------------------------------------------------
  virtual NABoolean operator==(const Attributes &other) const;

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual Int16 getClassSize() { return (Int16)sizeof(*this); }
  virtual char *findVTblPtr(Int16 classID);
  // ---------------------------------------------------------------------

  static int getFirstCharLength(const char *buf, int buflen, CharInfo::CharSet cs);

  static int convertOffsetToChar(const char *buf, int offset, CharInfo::CharSet cs);

  static int convertCharToOffset(const char *buf, int numOfChar, int maxBufLen, CharInfo::CharSet cs);

  static int getCharLengthInBuf(const char *buf, const char *endOfBuf, char *charLengthInBuf, CharInfo::CharSet cs);

  static int trimFillerSpaces(const char *buf, int precision, int maxBufLen, CharInfo::CharSet cs);

 private:
  enum flags_type {
    DATA_ALIGNMENT_FLAG = 0x0001,  // indicates that data alignment is needed at
                                   // runtime before expression evaluation.
    COMPLEX_TYPE = 0x0002,         // indicates that this is a complex type.
                                   // Used at unpack/fixup time to fixup
                                   // virtual table pointers.
    ADDED_COL = 0x0004,            // This indicates that the column being
                                   // processed is an added column.
                                   // It needs to be
                                   // handled in a special way.
                                   // See evalClauses() in exp_eval.cpp.
    SHOWPLAN_ = 0x0008,
    UPSHIFT_ = 0x0010,        // Upshifted char/varchar datatype.
    WIDECHAR_ = 0x0020,       // A wide char. Assumes 2 bytes per char.
                              // Could add a bytesPerChar_ field to
                              // SimpleType.But that would mean recreation
                              // of all tables. Avoiding that for now.
    GU_OUTPUT = 0x0040,       // isAGenericUpdateOutputFunction is TRUE.
                              // Need to be placed after varchar fields.
    LAST_DAY_MONTH = 0x0080,  // Used for last day of month processing for
                              // date/datetime intervals.
    LAST_DAY_ERROR = 0x0100,  // Throw an error if set when last day of month
                              // processing of date/datetime intervals used.
    BULK_MOVE_ = 0x0200,      // Whether bulk move is possible for this field

    CASEINSENSITIVE_ = 0x0400,  // caseinsensitive char/varchar datatype

    FORCE_FIXED_ = 0x0800,  // Force this attribute to be treated as fixed
                            // in an aligned row.  Used by HashGroupby for
                            // varchar aggregates

    LENGTH_IN_KB_ = 0x1000,  // Indicates length is in KB

    IS_HBASE_MAPTABLE_COL = 0x2000,  // table is the hbase map table

    SPACE_SENSITIVE_ = 0x4000  // Indicates whether data type is real varchar; eg.
                               // if the origin type is char, but after translate,
                               // type turn into varchar, this is not a real varchar
  };

  // default value associated with this datatype.
  NABasicPtr defaultValue_;  // 00-07
  UInt32 defaultFieldNum_;   // 08-11

  UInt32 flags_;          // 12-15
  UInt32 flags2_;         // 16-19
  int offset_;          // 20-23
  int nullIndOffset_;   // 24-27
  int vcLenIndOffset_;  // 28-31

  // offset into VOA[]
  UInt32 voaOffset_;  // 32-35

  // relative based on first fixed
  UInt32 relOffset_;  // 36-39

  // this is the next attribute index in disk order (only for SQLMX_FORMAT)
  UInt32 nextAttrIdx_;  // 40-43

  int rowsetSize_;  // 44-47

  Int16 atp_;       // 48-49
  Int16 atpindex_;  // 50-51

  // enum DefaultClass
  Int16 defClass_;  // 52-53

  Int16 datatype_;  // 54-55

  Int16 nullFlag_;  // 56-57

  // 2 bytes or 4 bytes of null indicator length
  Int16 nullIndicatorLength_;  // 58-59

  // size of the indicator bytes preceeding the actual varchar data.
  // Indicator bytes contain the actual length of vc data.
  Int16 vcIndicatorLength_;  // 60-61

  // ExpTupleDesc::TupleDataFormat
  Int16 tdf_;  // 62-63

  // aligned on 1, 2, 4, or 8 byte boundary
  Int16 alignment_;  // 64-65

  // Additional rowset information in the form of bits. If first bit is set, then
  // we use the whole rowset size, otherwise we use the size of its elements
  Int16 rowsetInfo_;  // 66 - 67

  // Used for aligned row format since null indicator bytes are now a null bitmap.
  Int16 nullBitIdx_;  // 68 - 69

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  UInt32 valId_;  // 70 - 73
  // here column name length 100 Depending on cqd HBASE_MAX_COLUMN_NAME_LENGTH
  char colName_[100];  // 74 - 173
  char fillers_[11];   // 174 - 183
};

// Following typedef is needed by .../exp/ExpPCodeOptsNativeExpr.cpp to
// resolve an ambiguity with an LLVM class that is also named Attributes
typedef Attributes exp_Attributes;

inline void Attributes::needDataAlignment() { flags_ |= DATA_ALIGNMENT_FLAG; }

inline void Attributes::dontNeedDataAlignment() { flags_ &= ~DATA_ALIGNMENT_FLAG; }

inline int Attributes::isNotAlwaysAligned() { return (flags_ & DATA_ALIGNMENT_FLAG); }

///////////////////////////////////////////////////////////////
// class SimpleType
//   These are system defined types, like INT, CHAR, etc.
//   Operations on these are supported by underlying hardware
//   and thus are performed as a 'fastpath'.
///////////////////////////////////////////////////////////////
class SimpleType : public Attributes {
 public:
  SimpleType(int length, short scale, int precision) : length_(length), scale_(scale), precision_(precision) {
    setCollation(CharInfo::DefaultCollation);
    setClassID(SimpleTypeID);
    memset(fillers_, 0, sizeof(fillers_));
  }

  SimpleType(int length, short scale, int precision, short collation)
      : length_(length), scale_(scale), precision_(precision), collation_(collation) {
    setClassID(SimpleTypeID);
    memset(fillers_, 0, sizeof(fillers_));
  }

  SimpleType(Int16 datatype, int length, Int16 scale, int precision, ExpTupleDesc::TupleDataFormat tdf,
             int alignment, Int16 nullFlag, Int16 nullIndicatorLen, Int16 vcIndicatorLen, DefaultClass defClass,
             Int16 upshift) {
    setClassID(SimpleTypeID);
    setLength(length);
    setScale(scale);
    setPrecision(precision);
    setCollation(CharInfo::DefaultCollation);
    setDatatype(datatype);
    setTupleFormat(tdf);
    setNullFlag(nullFlag);
    setNullIndicatorLength(nullIndicatorLen);
    setVCIndicatorLength(vcIndicatorLen);
    setDefaultClass(defClass);
    setUpshift(upshift);
    setDataAlignmentSize(alignment);
    memset(fillers_, 0, sizeof(fillers_));
  }

  SimpleType() {
    length_ = 0;
    scale_ = 0;
    precision_ = 0;
    setCollation(CharInfo::DefaultCollation);
    memset(fillers_, 0, sizeof(fillers_));
  }

  ~SimpleType() {}

  inline void setLength(int length) { length_ = length; }
  int getLength() { return length_; }

  inline void setScale(Int16 scale) { scale_ = scale; }
  Int16 getScale() { return scale_; }
  UInt16 getScaleAsUI() { return (UInt16)scale_; }

  int getPrecision() { return precision_; }
  inline void setPrecision(int precision) { precision_ = precision; }

  // overload member scale_ to store the charset.
  void setCharSet(CharInfo::CharSet charSet) { scale_ = charSet; }

  CharInfo::CharSet getCharSet() {
    if (scale_ == 0)
      return CharInfo::ISO88591;  // R1.x backward compatibility
    else
      return (CharInfo::CharSet)scale_;
  }

  CharInfo::Collation getCollation() { return (CharInfo::Collation)collation_; }

  void setCollation(CharInfo::Collation coll) { collation_ = (Int16)coll; }

  void setIsoMapping(CharInfo::CharSet isoMappingcs) { isoMapping_ = isoMappingcs; }

  CharInfo::CharSet getIsoMapping() { return (CharInfo::CharSet)isoMapping_; }

  void copyAttrs(Attributes *source_);  // copy source attrs to this.

  int getStorageLength() {
    int ret_length = length_;

    if (getNullFlag()) ret_length += getNullIndicatorLength();

    ret_length += getVCIndicatorLength();

    return ret_length;
  }

  //
  // Default values always stored with null bytes followed by variable length
  // (if applicable) followed by actual value.
  // Null bytes are always a fixed size independent of the actual data format.
  // Variable length may be 2 or 4 bytes depending on the actual data format.
  int getDefaultValueStorageLength() {
    int retLen = length_;

    if (getNullFlag()) retLen += ExpTupleDesc::NULL_INDICATOR_LENGTH;

    retLen += getVCIndicatorLength();

    return retLen;
  }

  Attributes *newCopy();
  Attributes *newCopy(CollHeap *);

  // ---------------------------------------------------------------------
  // Method for comparing if two Attributes are equal.
  // ---------------------------------------------------------------------
  virtual NABoolean operator==(const Attributes &other) const;

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    Attributes::populateImageVersionIDArray();
  }

  virtual Int16 getClassSize() { return (Int16)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int length_;  // 00-03
  //----------------------------------------------------------------------
  // The orginal type of precision_ is UINT16.
  // but when using SimpleType to describe a VARCHAR type, precision_
  // is treated as MaxNumChars.
  // can see: ExpGenerator::convertNATypeToAttributes().
  //
  // Here are some SQLs:
  //
  //  SQL1: CREATE TABLE t1 (a varchar(200000) character set utf8);
  //  SQL2: INSERT INTO t1 VALUES(lpad('a', 3393, 'a'));
  //
  // When SQL1 was done, i think the value of precision_ should be 200000,
  // but if the type of precision was UINT16(the maximum value of UINT16 is
  // 65536), the actual value of precision is 3392, it was truncated.
  // And then we do SQL2 statement, will get a warning(8402) and the data
  // which want to be inserted is also truncated.
  // can see: core/sql/exp_conv.cpp:convCharToChar()
  //
  // This should not happen, because of the max size in bytes of
  // a char or varchar column is 16M.
  //----------------------------------------------------------------------
  int precision_;   // 04-07
  Int16 scale_;       // 08-09
  Int16 isoMapping_;  // 10-11
  Int16 collation_;   // 12-13

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[2];  // 14-15
};

/////////////////////////////////////////////////////////////
// class ComplexType
//    These are complex types not supported by underlying
//    hardware. Like, large decimal, Big Num, IEEE floating point
//    etc. Operations on these classes are provided by
//    classes derived from ComplexType.
//
/////////////////////////////////////////////////////////////
class ComplexType : public Attributes {
 public:
  ComplexType() : Attributes(ComplexTypeID), complexDatatype_(-999) { memset(fillers_, 0, sizeof(fillers_)); }

  ~ComplexType() {}

  inline void setComplexDatatype(Int16 complexDatatype) { complexDatatype_ = complexDatatype; }

  inline Int16 getComplexDatatype() { return complexDatatype_; }

  virtual Int16 add(Attributes *, Attributes *, char * /*op_data*/[]) { return -1; }

  virtual Int16 sub(Attributes *, Attributes *, char * /*op_data*/[]) { return -1; }

  virtual Int16 mul(Attributes *, Attributes *, char * /*op_data*/[]) { return -1; }

  virtual Int16 div(Attributes *, Attributes *, char * /*op_data*/[], CollHeap *heap, ComDiagsArea **diagsArea) {
    ExRaiseSqlError(heap, diagsArea, EXE_INTERNAL_ERROR);
    return -1;
  }

  virtual Int16 hash(Attributes *, char * /*op_data*/[]) { return -1; }

  // returns  1, if "this_ <comp_op_> other_" is true,
  //          0, otherwise.
  virtual Int16 comp(OperatorTypeEnum /*comp_op*/, Attributes *, char * /*op_data*/[]) { return -1; }

  // converts 'other' to 'this'
  virtual Int16 conv(Attributes * /*other*/, char * /*op_data*/[]) { return -1; }

  // cast to a SimpleType
  virtual Int16 castTo(Attributes *, char * /*op_data*/[], CollHeap *heap, ComDiagsArea **diagsArea) { return -1; }

  // cast from a SimpleType
  virtual Int16 castFrom(Attributes *, char * /*op_data*/[], CollHeap *heap, ComDiagsArea **diagsArea) { return -1; }

  virtual int getStorageLength() { return -1; }

  virtual int getDefaultValueStorageLength() { return -1; }

  virtual int getLength() { return -1; }

  virtual Int16 getScale() { return -1; }

  virtual int getPrecision() { return -1; }

  virtual Attributes *newCopy() { return 0; }

  virtual Attributes *newCopy(CollHeap *) { return 0; }

  virtual void copyAttrs(Attributes *src) {}  // copy source attrs to this.

  virtual void encode(const char *inBuffer, char *outBuffer, Int16 descending) {}

  // if a complex datatype need some temp space at runtime to perform
  // arith operations (add, sub, etc), then this method is called at
  // code generation time to set the offset to the temp space
  // location. If the optional length parameter is passed in, then
  // the length of temp space is set to that value. Otherwise,
  // this method figures out the length and returns it as the
  // return value.
  // Note that in some places the offset is overloaded as length when the
  // 3rd parameter is absent, see generator/GenItemExpr.cpp
  virtual int setTempSpaceInfo(OperatorTypeEnum, ULong /*offset*/, int /*length*/ = 0) { return 0; }

  // ---------------------------------------------------------------------
  // Method for comparing if two Attributes are equal.
  // ---------------------------------------------------------------------
  virtual NABoolean operator==(const Attributes &other) const;

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    Attributes::populateImageVersionIDArray();
  }

  virtual Int16 getClassSize() { return (Int16)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  Int16 complexDatatype_;  // 00-01

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[22];  // 02-23
};

class ShowplanAttributes : public Attributes {
 public:
  ShowplanAttributes(int valueId, char *text);
  ShowplanAttributes() {}
  ~ShowplanAttributes();
  int &valueId() { return valueId_; };
  char *text() { return text_; };

  Attributes *newCopy();
  Attributes *newCopy(CollHeap *);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    Attributes::populateImageVersionIDArray();
  }

  virtual Int16 getClassSize() { return (Int16)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  int valueId_;  // 00-03
  char text_[56];  // 04-59
  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[4];  // 60-63
};

#endif
