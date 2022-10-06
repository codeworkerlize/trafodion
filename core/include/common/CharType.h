
#ifndef CHARTYPE_H
#define CHARTYPE_H
/* -*-C++-*-
**************************************************************************
*
* File:         CharType.h
* Description:  Character Type
* Created:      4/27/94
* Language:     C++
*
*
**************************************************************************
*/

#include <limits.h>

#include "common/BaseTypes.h"
// Some ANSI compilers require this function to be declared even though
// the template NAKeyLookup is not instantiated (that class template is a
// subclass of NAHashDictionary ... in /common/Collections.h )
int hashKey();  // a dummy -- hopefully the linker won't look for it ...
#include "common/ComSmallDefs.h"
#include "common/NAType.h"
#include "common/charinfo.h"
#include "common/nawstring.h"
#include "common/str.h"
#include "exp/ExpLOBenums.h"
// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class CharLenInfo;
class CharType;
class SQLChar;
class SQLVarChar;
class AnsiChar;
class SQLLongVarChar;

// ***********************************************************************
//
//  CharLenInfo : Information about the length of the character data type
//
// ***********************************************************************
class CharLenInfo {
 public:
  CharLenInfo() : maxLenInChars_(0), maxLenInBytes_(0) {}
  CharLenInfo(const CharLenInfo &rhs) : maxLenInChars_(rhs.maxLenInChars_), maxLenInBytes_(rhs.maxLenInBytes_) {}
  CharLenInfo(const int maxLenInChars, const int maxLenInBytes)
      : maxLenInChars_(maxLenInChars), maxLenInBytes_(maxLenInBytes) {}

  ~CharLenInfo() {}

  int getMaxLenInChars() const { return maxLenInChars_; }
  int getMaxLenInBytes() const { return maxLenInBytes_; }

  void setMaxLenInChars(int maxLenInChars) { maxLenInChars_ = maxLenInChars; }
  void setMaxLenInBytes(int maxLenInBytes) { maxLenInBytes_ = maxLenInBytes; }

 private:
  int maxLenInChars_;  // for UCS2, # of UCS2 elements - for other, # of real (e.g., UCS4) characters
  int maxLenInBytes_;  // count in bytes - important when cs is a variable-length mbcs like UTF-8
};

// ***********************************************************************
//
//  CharType : The character data type
//
// ***********************************************************************
class CharType : public NAType {
 public:
  // ---------------------------------------------------------------------
  // ## Future work is required to encapsulate and process information for
  // ## Collations, Translations, Character Set Specifications/Repertoires.
  // ## See CharType::computeNextKeyValue() for an example of one method
  // ## (at least) that needs rework to handle these things.
  // ---------------------------------------------------------------------

  // CharType was designed for column declaration. However, we have extend the
  // character set & collate clauses to SCHEMA & CATALOG. Adding them to the adtName list, which
  // doesn't seem to serve any purpose, as far as I can tell.
  static const NAString LiteralSchema;
  static const NAString LiteralCatalog;

  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  CharType(NAMemory *h, const NAString &adtName, int maxCharStrLen, short bytesPerChar = 0,
           NABoolean nullTerminated = FALSE, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
           NABoolean isCaseInsensitive = FALSE, NABoolean varLenFlag = FALSE,
           CharInfo::CharSet cs = CharInfo::DefaultCharSet, CharInfo::Collation co = CharInfo::DefaultCollation,
           CharInfo::Coercibility ce = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet,
           int vcIndLen = 0  // not passed in, need to be computed
  );
  CharType(NAMemory *h, const NAString &adtName, const CharLenInfo &maxLenInfo,
           short /*max*/ bytesPerChar = 0,  // is maxBytesPerChar when cs is SJIS or UTF8
           NABoolean nullTerminated = FALSE, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
           NABoolean isCaseInsensitive = FALSE, NABoolean varLenFlag = FALSE,
           CharInfo::CharSet cs = CharInfo::DefaultCharSet, CharInfo::Collation co = CharInfo::DefaultCollation,
           CharInfo::Coercibility ce = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet);
  // copy ctor
  CharType(const CharType &charType, NAMemory *heap)
      : NAType(charType, heap),
        qualifier_(CHARACTER_STRING_TYPE),
        bytesPerChar_(charType.bytesPerChar_),
        charLimitInUCS2or4chars_(charType.charLimitInUCS2or4chars_),
        nullTerminated_(charType.nullTerminated_),
        upshifted_(charType.upshifted_),
        caseinsensitive_(charType.caseinsensitive_),
        charSet_(charType.charSet_),
        collation_(charType.collation_),
        coercibility_(charType.coercibility_),
        encodingCharSet_(charType.encodingCharSet_),
        isVarchar2_(charType.isVarchar2_),
        isSpaceSensitive_(charType.isSpaceSensitive_) {}
  virtual short getFSDatatype() const;
  virtual int getPrecisionOrMaxNumChars() const;
  virtual int getScaleOrCharset() const;

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  virtual CharInfo::CharSet getCharSet() const { return charSet_; }
  CharInfo::Collation getCollation() const { return collation_; }
  CharInfo::Coercibility getCoercibility() const { return coercibility_; }
  CharInfo::CharSet getEncodingCharSet() const { return encodingCharSet_; }

  void setCharSet(CharInfo::CharSet cs) { charSet_ = cs; }
  void setEncodingCharSet(CharInfo::CharSet cs) { encodingCharSet_ = cs; }
  void setCollation(CharInfo::Collation co) { collation_ = co; }
  void setCoercibility(CharInfo::Coercibility ce) {
    if (ce == CharInfo::IMPLICIT) ce = CharInfo::COERCIBLE;
    coercibility_ = ce;
  }
  void setCoAndCo(CharInfo::Collation co, CharInfo::Coercibility ce) {
    setCollation(co);
    setCoercibility(ce);
  }

  void setBytesPerChar(short BpC) { bytesPerChar_ = BpC; }
  short getBytesPerChar() const { return bytesPerChar_; }
  NABoolean isNullTerminated() const { return nullTerminated_; }
  NABoolean isUpshifted() const { return upshifted_; }
  void setUpshifted(NABoolean us) { upshifted_ = us; }
  NABoolean isCaseinsensitive() const { return caseinsensitive_; }
  void setCaseinsensitive(NABoolean ci) { caseinsensitive_ = ci; }
  void setDataStorageSize(int size) { dataStorageSize_ = size; }
  int getDataStorageSize() const { return dataStorageSize_; }
  int getStrCharLimit() const {
    if (getNominalSize() > 0 && getCharLimitInUCS2or4chars() > 0 && (getCharSet() == CharInfo::UTF8))
      return getCharLimitInUCS2or4chars();

    if (getCharSet() == CharInfo::UNICODE)
      return getNominalSize() / getBytesPerChar();
    else
      return getNominalSize();
  }
  int getMaxLenInBytesOrNAWChars() const {
    if (getCharSet() == CharInfo::UNICODE)
      return getNominalSize() / getBytesPerChar();
    else
      return getNominalSize();
  }

  int getCharLimitInUCS2or4chars() const { return charLimitInUCS2or4chars_; }

  // Useful for testing compatibility with double-byte char's
  NABoolean sizeIsEven() const {
    int rem = getNominalSize() % 2;
    if (isNullTerminated() && getBytesPerChar() == 1) return (rem == 1);
    return (rem == 0);
  }

  // the blank character is used to fill strings when comparing strings of
  // different length or when converting a shorter string to a longer one
  virtual int getBlankCharacterValue() const;

  // get the smallest and the greatest single character in this
  // character set and collation
  int getMinSingleCharacterValue() const;
  int getMaxSingleCharacterValue() const;

  // encode a string into a double precision, preserving the order of values
  double encodeString(const char *str, int strLen) const;

  NAString *convertToString(double v, NAMemory *h) const;

  // ---------------------------------------------------------------------
  // Virtual functions that are given an implementation for this class.
  // ---------------------------------------------------------------------
  virtual NABoolean computeNextKeyValue(NAString &keyValue) const;
  virtual NABoolean computeNextKeyValue(NAWString &keyValue) const;
  virtual NABoolean computeNextKeyValue_UTF8(NAString &keyValue) const;

  virtual NAString getTypeSQLname(NABoolean terse = FALSE) const;

  virtual NAString getSimpleTypeName() const;

  virtual NAString getSPSQLTypeName() const;

  virtual NAString *getKey(CollHeap *h = 0) const;  // compute hash key from SQL name

  virtual NABoolean operator==(const NAType &other) const;

  // compares sttributes specific to char datatypes
  virtual NABoolean compareCharAttrs(const NAType &other) const;

  virtual NABoolean isCompatible(const NAType &other, UInt32 *flags = NULL) const;

  virtual NABoolean equalIgnoreCoercibility(const NAType &other) const;

#ifndef CLI_SRL
  virtual NABoolean isComparable(const NAType &other, ItemExpr *parentOp, int emitErr = EmitErrAlways,
                                 UInt32 *flags = NULL) const;
#endif

  NABoolean isCharSetAndCollationComboOK() const;
  NABoolean isCompatibleAllowUnknownCharset(const NAType &other) const;

  // ---------------------------------------------------------------------
  // A virtual function for synthesizing the type of a binary operator.
  // ---------------------------------------------------------------------
  virtual const NAType *synthesizeType(NATypeSynthRuleEnum synthRule, const NAType &operand1, const NAType &operand2,
                                       CollHeap *h, UInt32 *flags = NULL) const;

  // ---------------------------------------------------------------------
  // Print function for debugging
  // ---------------------------------------------------------------------
  virtual void print(FILE *ofd = stdout, const char *indent = DEFAULT_INDENT);

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) CharType(*this, h); }

  // A virtual function to return a varchar equivalent of this type
  virtual NAType *equivalentVarCharType(NAMemory *h, NABoolean quantizeLen = FALSE) { return this; }
  virtual NAType *equivalentCharType(NAMemory *h, NABoolean quantizeLen = FALSE) { return this; }

  // ---------------------------------------------------------------------
  // Need encoding for Unicode charset. // 7/27/98
  // See NAType.h for a description on this method.
  // ---------------------------------------------------------------------
  NABoolean isEncodingNeeded() const;

  // ---------------------------------------------------------------------
  // A method which tells if a conversion error can occur when converting
  // a value of this type to the target type.
  // ---------------------------------------------------------------------
  NABoolean errorsCanOccur(const NAType &target, NABoolean lax = TRUE) const;

  // ---------------------------------------------------------------------
  // Compute the collating sequence and coercibility for dyadic operators
  // ---------------------------------------------------------------------
  NABoolean computeCoAndCo(const CharType &other, CharInfo::Collation &co, CharInfo::Coercibility &ce) const;

  // ---------------------------------------------------------------------
  // Get the char type information as NAString.
  // ---------------------------------------------------------------------
  NAString getCharSetName() const;
  NAString getEncodingCharSetName() const;
  NAString getCollationName() const;

  NAString getCharSetAsPrefix() const;
  static NAString getCharSetAsPrefix(CharInfo::CharSet cs);
  static NAString getCollateClause(CharInfo::Collation co);
  static NAString getCoercibilityText(CharInfo::Coercibility ce);

  // ---------------------------------------------------------------------
  // CharSet info inference
  // ---------------------------------------------------------------------
  static const CharType *findPushDownCharType(CharInfo::CharSet, const CharType *, ...);
  static const CharType *desiredCharType(enum CharInfo::CharSet);

  // -----------------------------------------------------------------------
  // CHARACTER SET UTF8 character data type
  // -----------------------------------------------------------------------
  void generateTextThenSetDisplayDataType(CharInfo::CharSet cs  // in
                                          ,
                                          NAString &ddt  // in/out
  );

  virtual NABoolean createSQLLiteral(const char *buf, NAString *&sqlLiteral, NABoolean &isNull, CollHeap *h) const;

  static int determineVCHdrSize(int len);

  NABoolean isVarchar2() const { return isVarchar2_; }
  void setVarchar2(NABoolean b) { isVarchar2_ = b; }

  NABoolean isSpaceSensitive() const { return isSpaceSensitive_; }
  void setSpaceSensitive(NABoolean isSpaceSensitive) { isSpaceSensitive_ = isSpaceSensitive; }

 protected:
  void minMaxRepresentableValue(void *bufPtr, int *bufLen, NAString **stringLiteral, NABoolean isMax,
                                CollHeap *h) const;

 private:
  void NONVIRT_METHOD_PLACEHOLDER_1();  // slot for reuse, or delete it!##

  NABoolean operator=(const CharType &other);  // assignmentOp, not implemented

  // ---------------------------------------------------------------------
  // Declaration for different data type names that are used internally.
  //##	There is only one type being used currently -- CHARACTER_STRING_TYPE.
  //##	If more values are added to this, then the CharType methods
  //##	  operator==()  isCompatible()  isComparable()
  //##	may or may not need to be changed to use the CharTypeEnum qualifier_
  //##	as a discriminant...
  // ---------------------------------------------------------------------
  enum CharTypeEnum { MIN_CHAR_TYPE, CHARACTER_STRING_TYPE, MAX_CHAR_TYPE };

  // ---------------------------------------------------------------------
  // Definition of variable for storing internal form names.
  //##	See note at "enum CharTypeEnum" above.
  // ---------------------------------------------------------------------
  CharTypeEnum qualifier_;

  // ---------------------------------------------------------------------
  // Number of bytes per character - For variable-length multi-byte
  // character sets like UTF8 (a character may take 1, 2, 3 or 4 bytes),
  // this field contains the maximum number of bytes per character.
  // ---------------------------------------------------------------------
  short /*max*/ bytesPerChar_;

  // ---------------------------------------------------------------------
  // --- Maximum size in UCS2 or UCS4 characters ----
  // For getCharSet() == CharInfo::UCS2, the unit is UCS2 (or NAWchar),
  //     and surrogate pair has a count of 2 NAWchar(acters).
  // For variable-length multi-byte character sets like SJIS and UTF8,
  //     the unit is UCS4 (the actual character specified by the user).
  // Set this field to -1 if it is not used.
  // ---------------------------------------------------------------------
  int charLimitInUCS2or4chars_;

  // ---------------------------------------------------------------------
  // TRUE if these character strings are terminated using C nul character
  // (either byte or double-byte).  Implies no varLen header word.
  // ---------------------------------------------------------------------
  NABoolean nullTerminated_;

  // ---------------------------------------------------------------------
  // Flag indicating whether or not the string is "upshifted".
  // ---------------------------------------------------------------------
  NABoolean upshifted_;

  // ---------------------------------------------------------------------
  // Flag indicating whether or not the string is "caseinsensitive".
  // ---------------------------------------------------------------------
  NABoolean caseinsensitive_;

  CharInfo::CharSet charSet_;
  CharInfo::Collation collation_;
  CharInfo::Coercibility coercibility_;
  CharInfo::CharSet encodingCharSet_;

  NABoolean isVarchar2_;
  NABoolean isSpaceSensitive_;

};  // class CharType

// ***********************************************************************
//
//  SQLChar : SQL CHARACTER
//
// ***********************************************************************
class SQLChar : public CharType {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SQLChar(NAMemory *h, int maxLength, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
          NABoolean isCaseInsensitive = FALSE, NABoolean varLenFlag = FALSE,
          CharInfo::CharSet = CharInfo::DefaultCharSet, CharInfo::Collation = CharInfo::DefaultCollation,
          CharInfo::Coercibility = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet);
  SQLChar(NAMemory *h, const CharLenInfo &maxLenInfo, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
          NABoolean isCaseInsensitive = FALSE, NABoolean varLenFlag = FALSE,
          CharInfo::CharSet = CharInfo::DefaultCharSet, CharInfo::Collation = CharInfo::DefaultCollation,
          CharInfo::Coercibility = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet);
  // copy ctor
  SQLChar(const SQLChar &sqlChar, NAMemory *heap) : CharType(sqlChar, heap) {}
  short getFSDatatype() const;

  // ---------------------------------------------------------------------
  // Methods that return the binary form of the minimum and the maximum
  // representable values.
  // ---------------------------------------------------------------------
  virtual void minRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;
  virtual void maxRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;
  // get the encoding of the max char value
  double getMaxValue() const;

  // get the encoding of the min char value
  double getMinValue() const;

  virtual double encode(void *) const;
  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) SQLChar(*this, h); }

  // A virtual function to return a varchar equivalent of this type
  virtual NAType *equivalentVarCharType(NAMemory *h, NABoolean quantizeLen = FALSE);

 private:
};  // class SQLChar

// ***********************************************************************
//
//  SQLVarChar : SQL CHARACTER VARYING
//
// ***********************************************************************
class SQLVarChar : public CharType {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SQLVarChar(NAMemory *h, int maxLength, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
             NABoolean isCaseInsensitive = FALSE, CharInfo::CharSet = CharInfo::DefaultCharSet,
             CharInfo::Collation = CharInfo::DefaultCollation, CharInfo::Coercibility = CharInfo::COERCIBLE,
             CharInfo::CharSet encoding = CharInfo::UnknownCharSet, int vcIndLen = 0);
  SQLVarChar(NAMemory *h, const CharLenInfo &maxLenInfo, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
             NABoolean isCaseInsensitive = FALSE, CharInfo::CharSet = CharInfo::DefaultCharSet,
             CharInfo::Collation = CharInfo::DefaultCollation, CharInfo::Coercibility = CharInfo::COERCIBLE,
             CharInfo::CharSet encoding = CharInfo::UnknownCharSet);

  SQLVarChar(const SQLChar &fixed);
  // copy ctor
  SQLVarChar(const SQLVarChar &varChar, NAMemory *heap)
      : CharType(varChar, heap), clientDataType_(varChar.getClientDataTypeName(), heap) {}
  virtual void minRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;
  virtual void maxRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;

  // True FS data type is the type that the object gets in the sqlparser.
  // That type usually the same through out the life span of the object: from
  // the compiler to the executor. For LONGWVARCHAR, the fs type is
  // REC_BYTE_V_ASCII_LONG as assigned by the parser, and that type is later
  // changed to REC_NCHAR_V_UNICODE.
  //
  // This new method allows the reuse of code for REC_NCHAR_V_UNICODE for
  // REC_BYTE_V_ASCII_LONG.
  virtual short getTrueFSDatatype() const { return getFSDatatype(); };
  virtual short getFSDatatype() const;

  virtual double encode(void *) const;

  virtual NABoolean isEncodingNeeded() const { return TRUE; }

  virtual NABoolean operator==(const NAType &other) const;

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) SQLVarChar(*this, h); }
  // A virtual function to return a char equivalent of this type
  virtual NAType *equivalentCharType(NAMemory *h, NABoolean quantizeLen = FALSE);

  NAString getClientDataTypeName() const { return clientDataType_.isNull() ? getSimpleTypeName() : clientDataType_; }

  void setClientDataType(NAString clientName) { clientDataType_ = clientName; }

 private:
  NAString clientDataType_;
};  // class SQLVarChar

// ***********************************************************************
//
//  ANSIChar : ANSI CHARACTER
//              Null terminated string. Not supported internally.
//              Valid for application hostvar and params only.
//
// ***********************************************************************
class ANSIChar : public CharType {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  ANSIChar(NAMemory *h, int maxLength, NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE,
           NABoolean varLenFlag = FALSE, CharInfo::CharSet = CharInfo::DefaultCharSet,
           CharInfo::Collation = CharInfo::DefaultCollation, CharInfo::Coercibility = CharInfo::COERCIBLE,
           CharInfo::CharSet encoding = CharInfo::UnknownCharSet);
  ANSIChar(const ANSIChar &ansiChar, NAMemory *heap) : CharType(ansiChar, heap) {}
  short getFSDatatype() const;

  NABoolean isExternalType() const { return TRUE; }

  NAType *equivalentType(CollHeap *h = 0) const {
    return new (h) SQLVarChar(h, getNominalSize(), supportsSQLnull(), isUpshifted(), FALSE, getCharSet(),
                              getCollation(), getCoercibility());
  }

  // A virtual function to return a varchar equivalent of this type
  virtual NAType *equivalentVarCharType(NAMemory *h, NABoolean quantizeLen = FALSE);
  virtual NAType *equivalentCharType(NAMemory *h, NABoolean quantizeLen = FALSE) { return NULL; }

  // ---------------------------------------------------------------------
  // Methods that returns the binary form of the minimum and the maximum
  // representable values.
  // ---------------------------------------------------------------------
  virtual void minRepresentableValue(void *, int *, NAString ** /*stringLiteral*/ = NULL, CollHeap * = 0) const {};

  virtual void maxRepresentableValue(void *, int *, NAString ** /*stringLiteral*/ = NULL, CollHeap * = 0) const {};

  virtual double encode(void *) const { return 0E0; }

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) ANSIChar(*this, h); }

 private:
};  // class ANSIChar

// **********************************************************************
//
// SQLLONGVARCHAR: SQLVARCHAR
// Purpose: This class was added to provide support for the OBDC datatype.
//
// **********************************************************************

class SQLLongVarChar : public SQLVarChar {
 public:
  SQLLongVarChar(NAMemory *h, int maxLength, NABoolean validLength = FALSE, NABoolean allowSQLnull = TRUE,
                 NABoolean isUpShifted = FALSE, NABoolean isCaseInsensitive = FALSE,
                 CharInfo::CharSet cs = CharInfo::DefaultCharSet, CharInfo::Collation co = CharInfo::DefaultCollation,
                 CharInfo::Coercibility ce = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet)
      : SQLVarChar(h, maxLength, allowSQLnull, isUpShifted, isCaseInsensitive, cs, co, ce) {
    setClientDataType("LONG VARCHAR");
    lengthNotSet_ = !validLength;
  }
  SQLLongVarChar(NAMemory *h, const CharLenInfo &maxLenInfo, NABoolean validLength = FALSE,
                 NABoolean allowSQLnull = TRUE, NABoolean isUpShifted = FALSE, NABoolean isCaseInsensitive = FALSE,
                 CharInfo::CharSet cs = CharInfo::DefaultCharSet, CharInfo::Collation co = CharInfo::DefaultCollation,
                 CharInfo::Coercibility ce = CharInfo::COERCIBLE, CharInfo::CharSet encoding = CharInfo::UnknownCharSet)
      : SQLVarChar(h, maxLenInfo, allowSQLnull, isUpShifted, isCaseInsensitive, cs, co, ce) {
    setClientDataType("LONG VARCHAR");
    lengthNotSet_ = !validLength;
  }
  SQLLongVarChar(const SQLLongVarChar &aLongVarChar, NAMemory *heap)
      : SQLVarChar(aLongVarChar, heap), lengthNotSet_(aLongVarChar.isLengthNotSet()) {}
  virtual short getTrueFSDatatype() const;
  virtual short getFSDatatype() const;

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) SQLLongVarChar(*this, h); }

  NABoolean isLengthNotSet() const { return lengthNotSet_; }

  void setLength(int size) {
    dataStorageSize_ = size;
    lengthNotSet_ = FALSE;
  }

 private:
  NABoolean lengthNotSet_;

};  // class SQLLongVarChar

// **********************************************************************
//
// SQLLOB:
// Purpose: This class was added to provide support for the LOB datatype.
//
// **********************************************************************

class SQLlob : public NAType {
 public:
  SQLlob(NAMemory *h, NABuiltInTypeEnum ev, long lobLength, ComLobsStorageType lobStorage, NABoolean allowSQLnull,
         int lobHandleLen);
  SQLlob(const SQLlob &aLob, NAMemory *heap) : NAType(aLob, heap) {
    lobLength_ = aLob.lobLength_;
    lobStorage_ = aLob.lobStorage_;
    lobHandleLen_ = aLob.lobHandleLen_;
    setCharSet(CharInfo::ISO88591);  // default
  }

  // is this type a blob/clob
  virtual NABoolean isLob() const { return TRUE; };

  // ---------------------------------------------------------------------
  // A method which tells if a conversion error can occur when converting
  // a value of this type to the target type.
  // ---------------------------------------------------------------------
  NABoolean errorsCanOccur(const NAType &target, NABoolean lax = TRUE) const;

  // ---------------------------------------------------------------------
  // A name that expresses the characteristics of the type.
  // ---------------------------------------------------------------------
  virtual NAString getTypeSQLname(NABoolean terse = FALSE) const;

  long getLobLength() { return lobLength_; }
  int extFormatLen() { return lobHandleLen_; }
  void adjustLobHandleLen(int v) { lobHandleLen_ += v; }

  virtual int getPrecision() const { return lobLength_ >> 32; }
  virtual int getScale() const { return (int)lobLength_ & 0xFFFFFFFF; }
  virtual CharInfo::CharSet getCharSet() const { return charSet_; }
  void setCharSet(CharInfo::CharSet cs) { charSet_ = cs; }
  ComLobsStorageType getLobStorage() { return lobStorage_; }

 private:
  long lobLength_;
  int lobHandleLen_;
  CharInfo::CharSet charSet_;  // handle charset
  ComLobsStorageType lobStorage_;
};  // class SQLlob

// **********************************************************************
//
// SQLBLOB:
// Purpose: This class was added to provide support for the BLOB datatype.
//
// **********************************************************************

class SQLBlob : public SQLlob {
 public:
  SQLBlob(NAMemory *h, long blobLength, ComLobsStorageType lobStorage, NABoolean allowSQLnull, int lobHandleLen,
          NABoolean lobV2 = FALSE);
  SQLBlob(const SQLBlob &aBlob, NAMemory *heap) : SQLlob(aBlob, heap) {}
  virtual short getFSDatatype() const { return REC_BLOB; }

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const;

  const NAType *synthesizeType(NATypeSynthRuleEnum synthRule, const NAType &operand1, const NAType &operand2,
                               CollHeap *h, UInt32 *flags) const;

 private:
};  // class SQLBlob

// **********************************************************************
//
// SQLCLOB:
// Purpose: This class was added to provide support for the CLOB datatype.
//
// **********************************************************************

class SQLClob : public SQLlob {
 public:
  SQLClob(NAMemory *h, long blobLength, CharInfo::CharSet clobCharSet, ComLobsStorageType lobStorage,
          NABoolean allowSQLnull, int lobHandleLen, NABoolean lobV2 = FALSE);
  SQLClob(const SQLClob &aClob, NAMemory *heap) : SQLlob(aClob, heap) { setDataCharSet(aClob.getDataCharSet()); }
  virtual short getFSDatatype() const { return REC_CLOB; }
  virtual CharInfo::CharSet getDataCharSet() const { return dataCharSet_; }
  void setDataCharSet(CharInfo::CharSet cs) { dataCharSet_ = cs; }

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const;

  const NAType *synthesizeType(NATypeSynthRuleEnum synthRule, const NAType &operand1, const NAType &operand2,
                               CollHeap *h, UInt32 *flags) const;

 private:
  CharInfo::CharSet dataCharSet_;  // data charset
};                                 // class SQLClob

// ***********************************************************************
//
//  SQLBinaryString : SQL BINARY string
//
// ***********************************************************************
class SQLBinaryString : public CharType {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SQLBinaryString(NAMemory *h, int maxLength, NABoolean allowSQLnull = TRUE, NABoolean varLenFlag = FALSE);
  SQLBinaryString(const SQLBinaryString &sqlCharBinary, NAMemory *heap) : CharType(sqlCharBinary, heap) {}

  short getFSDatatype() const { return (isVaryingLen() ? REC_VARBINARY_STRING : REC_BINARY_STRING); }

  virtual NAString getTypeSQLname(NABoolean terse = FALSE) const;

  virtual NAType::SynthesisPrecedence getSynthesisPrecedence() const { return SYNTH_PREC_BINARY; }

  // get the encoding of the max char value
  double getMaxValue() const;

  // get the encoding of the min char value
  double getMinValue() const;

  virtual double encode(void *) const { return 0; }

  // ---------------------------------------------------------------------
  // Methods that return the binary form of the minimum and the maximum
  // representable values.
  // ---------------------------------------------------------------------
  virtual void minRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;
  virtual void maxRepresentableValue(void *, int *, NAString **stringLiteral = NULL, CollHeap *h = 0) const;

  virtual NABoolean isEncodingNeeded() const { return isVaryingLen(); }

  virtual NABoolean isCompatible(const NAType &other, UInt32 *flags) const;

  virtual NABoolean isComparable(const NAType &otherNA, ItemExpr *parentOp, int emitErr, UInt32 *flags) const;

  const NAType *synthesizeType(NATypeSynthRuleEnum synthRule, const NAType &operand1, const NAType &operand2,
                               CollHeap *h, UInt32 *flags) const;

  // ---------------------------------------------------------------------
  // A virtual function to return a copy of the type.
  // ---------------------------------------------------------------------
  virtual NAType *newCopy(NAMemory *h = 0) const { return new (h) SQLBinaryString(*this, h); }

 private:
};  // class SQLBinaryString

#endif /* CHARTYPE_H */
