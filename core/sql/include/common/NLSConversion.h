

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NLSConversion.h
 * RCS:          $Id:
 * Description:  The header file of a set of conversion functions
 *
 *
 * Created:      7/8/98
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef _NASTRINGCONVERSION_H
#define _NASTRINGCONVERSION_H

#include "common/Platform.h"

#if !defined(MODULE_DEBUG)

#include "common/BaseTypes.h"
#include "common/NAWinNT.h"
#include "common/NAHeap.h"
#include "common/stringBuf.h"
#include "common/csconvert.h"
#include "common/charinfo.h"

class NAString;
class NAWString;

NAWcharBuf *ISO88591ToUnicode(const charBuf &iso88591String, CollHeap *heap, NAWcharBuf *&unicodeString,
                              NABoolean addNullAtEnd = TRUE);

charBuf *unicodeToISO88591(const NAWcharBuf &unicodeString, CollHeap *heap, charBuf *&iso88591String,
                           NABoolean addNullAtEnd = TRUE, NABoolean allowInvalidCodePoint = TRUE);

NAWcharBuf *csetToUnicode(const charBuf &csetString, CollHeap *heap, NAWcharBuf *&unicodeString, int cset,
                          int &errorcode, NABoolean addNullAtEnd = TRUE, int *charCount = NULL,
                          int *errorByteOff = NULL);

charBuf *unicodeTocset(const NAWcharBuf &unicodeString, CollHeap *heap, charBuf *&csetString, int cset,
                       int &errorcode, NABoolean addNullAtEnd = TRUE, NABoolean allowInvalidCodePoint = TRUE,
                       int *charCount = NULL, int *errorByteOff = NULL);

NAWcharBuf *sjisToUnicode(const charBuf &sjisString, CollHeap *heap, NAWcharBuf *&unicodeString,
                          NABoolean addNullAtEnd = TRUE);

charBuf *unicodeToSjis(const NAWcharBuf &unicodeString, CollHeap *heap, charBuf *&sjisString,
                       NABoolean addNullAtEnd = TRUE, NABoolean allowInvalidCodePoint = TRUE);

charBuf *unicodeToUtf8(const NAWcharBuf &unicodeString, CollHeap *heap, charBuf *&utf8String,
                       NABoolean addNullAtEnd = TRUE, NABoolean allowInvalidCodePoint = TRUE);

NAWcharBuf *ksc5601ToUnicode(const charBuf &ksc5601String, CollHeap *heap, NAWcharBuf *&unicodeString,
                             NABoolean addNullAtEnd = TRUE);

charBuf *unicodeToKsc5601(const NAWcharBuf &unicodeString, CollHeap *heap, charBuf *&ksc5601String,
                          NABoolean addNullAtEnd = TRUE, NABoolean allowInvalidCodePoint = TRUE);

int unicodeToSjisChar(char *sjis, NAWchar wc);

cnv_charset convertCharsetEnum(int /*i.e. enum CharInfo::CharSet*/ inset);

const char *getCharsetAsString(int /*i.e. enum CharInfo::CharSet*/ charset);

int UnicodeStringToLocale(int /*i.e. enum CharInfo::CharSet*/ charset, const NAWchar *wstr, int wstrLen,
                            char *buf, int bufLen, NABoolean addNullAtEnd = TRUE,
                            NABoolean allowInvalidCodePoint = TRUE);

int LocaleStringToUnicode(int /*i.e. enum CharInfo::CharSet*/ charset, const char *str, int strLen,
                            NAWchar *wstrBuf, int wstrBufLen, NABoolean addNullAtEnd = TRUE);

int localeConvertToUTF8(char *source, int sourceLen, char *target, int targetLen,
                          int charset,  // enum cnv_charset type
                          CollHeap *heap = 0, int *charCount = NULL, int *errorByteOff = NULL);

int UTF8ConvertToLocale(char *source, int sourceLen, char *target, int targetLen,
                          int charset,  // enum cnv_charset type
                          CollHeap *heap = 0, int *charCount = NULL, int *errorByteOff = NULL);

// -----------------------------------------------------------------------
// ComputeWidthInBytesOfMbsForDisplay:
//
// Returns the display width (in bytes) that is the closest to the
// specified maximum display width (in bytes) without chopping the
// rightmost multi-byte characters into two parts so that we do not
// encounter the situation where the first part of the multi-byte
// character is in the current display line and the other part of
// the character is in the next display line.
//
// If encounters an error, return the error code (a negative number)
// define in w:/common/csconvert.h.
//
// In parameter pv_eCharSet contains the character set attribute
// of the input string passed in via the parameter pp_cMultiByteStr.
//
// The out parameter pr_iNumOfTranslatedChars contains the number of
// the actual (i.e., UCS4) characters translated.
//
// The out parameter pr_iNumOfNAWchars contains the number of UCS2
// characters (NAwchar[acters]) instead of the number of the actual
// (i.e., UCS4) characters.
//
// Note that classes NAMemory and CollHeap are the same except for
// the names.
// -----------------------------------------------------------------------
int ComputeWidthInBytesOfMbsForDisplay(const char *pp_cpMbs  // in
                                         ,
                                         const int pv_iMbsLenInBytes  // in
                                         ,
                                         const int pv_iMaxDisplayLenInBytes  // in
                                         ,
                                         const CharInfo::CharSet pv_eCharSet  // in
                                         ,
                                         int &pr_iNumOfTranslatedChars  // out - number of chars translated
                                         ,
                                         int &pr_iNumOfNAWchars  // out - width in NAWchar(acters)
                                         ,
                                         NAMemory *heap = NULL  // in  - default is process heap
);

// -----------------------------------------------------------------------
// ComputeStrLenInNAWchars:
//
// Returns the length of the input string (in the specified character set)
// in number of NAWchar(acters) - Note that a UTF16 character (i.e., a
// surrogate pair) will have a count of 2 NAWchar(acters).
//
// Return an error code (a negative number) if encounters an error.  The
// error code values are defined in w:/common/csconvert.h.
// -----------------------------------------------------------------------
int ComputeStrLenInNAWchars(const char *pStr, const int strLenInBytes, const CharInfo::CharSet cs,
                              NAMemory *workspaceHeap);

// -----------------------------------------------------------------------
// ComputeStrLenInUCS4chars:
//
// Returns the actual (i.e., UCS4) character count of the input string
// (in the specified character set) in the actual (i.e., UCS4) characters.
// Return an error code (a negative number) if encounters an error.  The
// error code values are defined in w:/common/csconvert.h.
// -----------------------------------------------------------------------
int ComputeStrLenInUCS4chars(const char *pStr, const int strLenInBytes, const CharInfo::CharSet cs);

// convert a Unicode string back to char
class NAMemory;
NAString *unicodeToChar(const NAWchar *s, int len, int charset, NAMemory *h = NULL,
                        NABoolean allowInvalidChar = FALSE);

// convert a char string to Unicode
NAWString *charToUnicode(int charset, const char *s, int len, NAMemory *h = NULL);
NAWString *charToUnicode(int charset, const char *s, NAMemory *h = NULL);

// convert a char string to another char string (in a different character set);
// if both target and source char sets are the same, do a deep copy.
NAString *charToChar(int targetCS, const char *s, int sLenInBytes, int sourceCS, NAMemory *h = NULL,
                     NABoolean allowInvalidChar = FALSE);

#else

#include <stdio.h>

typedef unsigned short NAWchar;
typedef NAWchar NAWchar;
typedef char CollHeap;
#define NABoolean int
#define TRUE      1
#define FALSE     0
#define NADELETEBASIC(buf_, heap_)
#define NADELETE(buf_, T_, heap_)
#include "common/stringBuf.h"

void *operator new(size_t size, CollHeap *s) {
  void *result = ::operator new(size);
  return result;
}

#endif

#endif
