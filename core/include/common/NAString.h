
#ifndef NASTRING_H
#define NASTRING_H
/* -*-C++-*-
******************************************************************************
*
* File:         NAString.h
* Description:  common definitions of basic data types
*
* Created:      4/27/94
* Language:     C++
*
*
******************************************************************************
*/

#include "common/BaseTypes.h"
#include "common/CollHeap.h"
#include "common/ComCharSetDefs.h"
#include "common/Int64.h"
#include "common/NAWinNT.h"
#include "export/NAStringDef.h"

// -----------------------------------------------------------------------
// a string datatype
// -----------------------------------------------------------------------

// NB: NAString no longer IS RWCString ; instead, it's a separate class,
// derived from RWCString, with all allocation changed from global new to
// instead use a CollHeap *

//#define NASTRING  RWCString	// needed by c89 compiler in a few places

// the class NAString methods are contained in NAStringDef.h/NAStringDef.cpp

// The following methods have to be defined for a NAString:
//
// - a default constructor (no length restrictions for the created string)
// - type conversion from and to char *
// - copy constructor
// - indexing operator char & operator[] (StringPos)
// - a substring operator NoahString operator() (StringPos start,StringPos len)
// - assignment operator (see below)
// - length operator StringPos length()
// - other methods as needed (use Tools.h++ method names)
//
// We expect the NAString data type to be implemented such that it "owns"
// the character string contained in it. That means that when a value is
// assigned to a NAString, then a copy is made of the source string such
// that the user of NAString is not required to keep the source of the
// assignment around. The copy of the string may be deleted at the next
// assignment, with the destructor, or in any other consistent way. See
// the Tools.h++ implementation of RWCString for an example on how this
// could work.

// We require a behavior like an array index from the StringPos data
// type. A type conversion (if necessary) must be defined such that integers
// from 0 upwards can be used to address string elements (astring[0] must
// access the first element).

// -----------------------------------------------------------------------
// Methods on strings (defined as standalone C procedures, since we have
// no control over the RWCString class.
//
// Note that convertNAString() allocates memory.
// -----------------------------------------------------------------------

char *convertNAString(const NAString &ns, CollHeap *heap, NABoolean wideNull = FALSE);
void GetSimplePosixFilename(NAString &filename, NABoolean doLower = FALSE);
int hashKey(const NAString &ns);
NABoolean IsNAStringSpace(const NAString &ns);
NABoolean IsNAStringSpaceOrEmpty(const NAString &ns);
NABoolean NAStringHasOnly7BitAsciiChars(const NAString &ns);
NABoolean NAStringHasOnlyDecimalDigitAsciiChars(const NAString &ns);
NABoolean IsSqlReservedWord(const char *sqlText);
NABoolean IsCIdentifier(const char *id);
NAString LookupDefineName(const NAString &ns, NABoolean iterate = FALSE);
void NAStringUpshiftASCII(NAString &ns);
int NAStringToLong(const NAString &ns);
double NAStringToReal(const NAString &ns);
UInt32 NAStringToUnsigned(const NAString &ns);
NAString LongToNAString(int l);
NAString RealToNAString(double d);
NAString UnsignedToNAString(UInt32 u);
NAString Int64ToNAString(long l);

// -----------------------------------------------------------------------
// ANSI SQL name related functions
// -----------------------------------------------------------------------

NAString ToAnsiIdentifier(const NAString &ns, NABoolean assertShort = TRUE);
char *ToAnsiIdentifier2(const char *nsData, size_t nsLen, CollHeap *heap);
char *ToAnsiIdentifier2(const char *nsData, size_t nsLen, CollHeap *heap, int isoMapCS);
void ToAnsiIdentifier3(const char *inputData, size_t inputLen, char *outputData, size_t outputMaxLen,
                       size_t *outputLen);
void ToAnsiIdentifier3(const char *inputData, size_t inputLen, char *outputData, size_t outputMaxLen, size_t *outputLen,
                       int isoMapCS);

// The following macro definitions are for the flags parameter of ToInternalIdentifier()
#define NASTRING_ALLOW_NSK_GUARDIAN_NAME_FORMAT   0x0001
#define NASTRING_REGULAR_IDENT_WITH_DOLLAR_PREFIX 0x0002
#define NASTRING_DELIM_IDENT_WITH_DOLLAR_PREFIX   0x0004
int ToInternalIdentifier(NAString &ansiIdent, int upCase = TRUE, NABoolean acceptCircumflex = FALSE,
                         UInt16 pv_flags = NASTRING_ALLOW_NSK_GUARDIAN_NAME_FORMAT);

// -----------------------------------------------------------------------
// SQL text related functions
// -----------------------------------------------------------------------

void ToInternalString(NAString &internalStr, const NAString &quotedStr);
void ToQuotedString(NAString &quotedStr, const NAString &internalStr, NABoolean encloseInQuotes = TRUE);
int PrettifySqlText(NAString &sqlText, const char *nationalCharSetName = NULL);
size_t LineBreakSqlText(NAString &sqlText, NABoolean showddlView = FALSE, size_t maxlen = 79,
                        size_t pfxlen = 2,      // indent subsequent lines 2 spaces
                        size_t pfxinitlen = 0,  // indent first line 0 spaces
                        char pfxchar = ' ', const char *schemaName = NULL, NABoolean commentOut = FALSE);

void TrimNAStringSpace(NAString &ns, NABoolean leading = TRUE, NABoolean trailing = TRUE);
size_t IndexOfFirstWhiteSpace(const NAString &ns, size_t startPos = 0);
size_t IndexOfFirstNonWhiteSpace(const NAString &ns, size_t startPos = 0);

void RemoveLeadingZeros(NAString &ns);
void RemoveTrailingZeros(NAString &ns);

NAString Latin1StrToUTF8(const NAString &latin1Str, NAMemory *heap = NULL);

NAString &replaceAll(NAString &source, const NAString &searchFor, const NAString &replaceWith);

class base64Decoder {
 public:
  base64Decoder();
  ~base64Decoder(){};

  NABoolean decode(const char *str, const size_t len, NAString &decoded);

 protected:
  int T_[256];
};

// See notes at NAString.cpp specialSQL_TEXT[].
//
// Also note that use of this non-Ansi value is going to break when we start
// supporting character sets beyond SQL_TEXT; then we will either have to have
// a function that returns an out-of-charset character for each given charset,
// and concatenate our internal unconflicting object names on the fly,
// rather than this nice simple hardcoded approach, OR
// change this '@' to '\001' or some such impossible-in-all-charsets character
// (as it's a nonprinting ascii char, using it will make debugging a tiny bit
// less obvious, that's all).
//
#define NON_SQL_TEXT_CHAR                     '@'
#define NON_SQL_TEXT_STR                      "@"
#define FUNNY_INTERNAL_IDENT(str)             str NON_SQL_TEXT_STR
#define FUNNY_ANSI_IDENT(str)                 "\"" str NON_SQL_TEXT_STR "\""
#define FUNNY_ANSI_IDENT_HAS_PREFIX(str, pfx) (strncmp(str, "\"" pfx, strlen("\"" pfx)) == 0)

void FUNNY_ANSI_IDENT_REMOVE_PREFIX(NAString &str, const char *pfx);

// The real Ark catalog manager returns all object names as three-part
// identifiers, properly delimited where necessary.
// Only simple column names are returned undelimited;
// only they should use this procedure.
#define ANSI_ID(name) ToAnsiIdentifier(name).data()

// Syntactic sugar to call Space::allocateAndCopyToAlignedSpace() in exp_space.C
// (because exe/exp cannot use NAString/RWCString stuff, since exe/exp
// is in a system library...)
#define AllocateAndCopyToAlignedSpace(/* const NAString& */ ns, /* size_t */ countPrefixSize) \
  allocateAndCopyToAlignedSpace((ns).data(), (ns).length(), countPrefixSize)

// -----------------------------------------------------------------------
// Routines to set and access the NAString_isoMappingCS memory cache for
// use by routines ToInternalIdentifier() and ToAnsiIdentifier[2|3]() in
// modules w:/common/NAString[2].cpp.  These routines currently cannot
// access SqlParser_ISO_MAPPING directly due to the complex build hierarchy.
// -----------------------------------------------------------------------
int NAString_getIsoMapCS();
void NAString_setIsoMapCS(int isoMappingCS);

// -----------------------------------------------------------------------
// Routines use by routines ToInternalIdentifier() and
// ToAnsiIdentifier[2|3]() in modules w:/common/NAString[2].cpp.
// -----------------------------------------------------------------------
inline SQLCHARSET_CODE ComGetNameInterfaceCharSet() { return SQLCHARSETCODE_UTF8; }

inline void ComSetNameInterfaceCharSet(SQLCHARSET_CODE ansiNameCharSet) {
  // ComASSERT(ansiNameCharSet == SQLCHARSETCODE_UTF8);
}

// -----------------------------------------------------------------------
// if NAString_getIsoMapCS() == (int)SQLCHARSETCODE_ISO88591
//    call the corresponding ...8859_1() routine
// otherwise only execute the corresponding 7-bit ASCII logic
NABoolean isUpperIsoMapCS(unsigned char c);
NABoolean isAlphaIsoMapCS(unsigned char c);
NABoolean isAlNumIsoMapCS(unsigned char c);
void NAStringUpshiftIsoMapCS(NAString &ns);

typedef LIST(const NAString *) ConstStringList;

#endif /* NASTRING_H */

// -----------------------------------------------------------------------
// This is necessary if you want to make a NAKeyLookup (hash table)
// containing elements that are NAString only.
// -----------------------------------------------------------------------
#ifdef USING_NAKEYLOOKUPONNASTRINGONLY
#ifndef NAKEYLOOKUPONNASTRINGONLY_H
#define NAKEYLOOKUPONNASTRINGONLY_H

#include "common/Collections.h"

class NAStringAsOnlyValueInNAKeyLookup : public NAString {
 public:
  NAStringAsOnlyValueInNAKeyLookup() : NAString() {}
  NAStringAsOnlyValueInNAKeyLookup(const char *str) : NAString(str) {}
  NAStringAsOnlyValueInNAKeyLookup(const NAString &str) : NAString(str) {}

  const NAString *getKey() const { return this; }
  // inherits NAString::operator==()
  // likewise hashkey(const NAString&) will be used by NAKeyLookup
};

class NAKeyLookupOnNAStringOnly : public NAKeyLookup<NAString, NAStringAsOnlyValueInNAKeyLookup> {
 public:
  NAKeyLookupOnNAStringOnly(short initSize = 29, CollHeap *h = NULL)
      : NAKeyLookup<NAString, NAStringAsOnlyValueInNAKeyLookup>(initSize, NAKeyLookupEnums::KEY_INSIDE_VALUE, h) {}

  // copy ctor
  NAKeyLookupOnNAStringOnly(const NAKeyLookupOnNAStringOnly &orig, CollHeap *h = 0);

  ~NAKeyLookupOnNAStringOnly() {
    //## This is commented out because it causes the dtor to crash.
    //## The only current user of this class wants this hashtable to
    //## persist over the entire lifetime of arkcmp, so memory leakage
    //## is not an issue for it.  But other (future) users, beware!
    //##
    //##	clearAndDestroy();
  }
};

#endif /* NAKEYLOOKUPONNASTRINGONLY_H     */
#endif /* USING_NAKEYLOOKUPONNASTRINGONLY */
