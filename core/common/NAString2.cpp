
/* -*-C++-*-
**************************************************************************
*
* File:         NAString2.C
* Description:  Utility string functions that can be run ONLY in a TRUSTED .exe
* Language:     C++
*
*
**************************************************************************
*/

#include "common/Platform.h"

//#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComAnsiNamePart.h"
#include "common/ComRtUtils.h"
//#include "common/ComSmallDefs.h"
#include "cli/sqlcli.h"
#include "common/ComSizeDefs.h"
#include "common/NAString.h"
#include "common/NAWinNT.h"
#include "common/nawstring.h"
#include "common/str.h"
#include "export/NABasicObject.h"

// move this method to NAString.h later.
NABoolean setMPLoc();

// -----------------------------------------------------------------------
static NABoolean NAString2_isUpper(unsigned char c, SQLCHARSET_CODE isoMapCS) {
  { return isUpper8859_1((NAWchar)c); }
  return FALSE;  // dead code
}

// -----------------------------------------------------------------------
static NABoolean NAString2_isRegularStart(unsigned char c, SQLCHARSET_CODE isoMapCS) {
  if (NAString2_isUpper(c, isoMapCS)) return TRUE;
  if (isoMapCS == SQLCHARSETCODE_ISO88591 AND isCaseInsensitive8859_1((NAWchar)c)) {
    return TRUE;
  }
  return FALSE;
}

// -----------------------------------------------------------------------
static NABoolean NAString2_isRegular(unsigned char c, SQLCHARSET_CODE isoMapCS) {
  if (NAString2_isRegularStart(c, isoMapCS)) return TRUE;
  if (isDigit8859_1((NAWchar)c) OR c == '_') return TRUE;
  return FALSE;
}

// -----------------------------------------------------------------------
// callers to this method must make sure that nsData points to a
// valid string with length greater than 0.
char *ToAnsiIdentifier2(const char *nsData, size_t nsLen, CollHeap *heap) {
  return ToAnsiIdentifier2(nsData, nsLen, heap, ComRtGetIsoMappingEnum());
}

// -----------------------------------------------------------------------
char *ToAnsiIdentifier2(const char *nsData, size_t nsLen, CollHeap *heap, int isoMapCS) {
  const int SMAX = 2048;  // See ToAnsiIdentifier for explanation of this const.

  if ((nsData == NULL) || (nsLen == 0) || (nsLen > SMAX)) return NULL;

  char buf[SMAX];
  size_t len;

  ToAnsiIdentifier3(nsData, nsLen, buf, SMAX, &len, isoMapCS);

  if (!len) return NULL;

  char *retBuf = new (heap) char[len + 1];
  str_cpy_all(retBuf, buf, len);
  retBuf[len] = 0;

  return retBuf;

}  // ToAnsiIdentifier2

// -----------------------------------------------------------------------

// In contrast to ToAnsiIdentifier2, the caller must
// pass in an output buffer of sufficient size

void ToAnsiIdentifier3(const char *inputData, size_t inputLen, char *outputData, size_t outputMaxLen,
                       size_t *outputLen) {
  ToAnsiIdentifier3(inputData, inputLen, /* ComGetNameInterfaceCharSet(), */
                    outputData, outputMaxLen, outputLen, ComRtGetIsoMappingEnum());
}

// -----------------------------------------------------------------------
void ToAnsiIdentifier3(const char *inputData, size_t inputLen, char *outputData, size_t outputMaxLen, size_t *outputLen,
                       int isoMapCS) {
  const int SMAX = 2048;  // See ToAnsiIdentifier for explanation of this const.

  if ((inputData == NULL) || (inputLen == 0) || (inputLen > SMAX) || (outputMaxLen < inputLen)) {
    *outputLen = 0;
    return;
  }

  // Fix for Bugzilla 2319 - BEGIN
  //
  // MXOSRVR calls CLI routines that may pass a name with trailing space(s) to the
  // CatMapAnsiNameToGuardianName() routine call which indirectly calls this routine.
  // Exclude the unwanted trailing spaces in inputData from the count.
  //
  int j = (int)inputLen - 1;
  if (j >= 0 && isSpace8859_1((unsigned char)inputData[j]))  // found a trailing space
  {
    for (j--; j >= 0 && isSpace8859_1((unsigned char)inputData[j]); j--)
      ;              // scan backward until a non-blank character is found
    if (j + 1 <= 0)  // inputData contains space(s) only
    {
      *outputLen = 0;
      return;
    }
    inputLen = (size_t)(j + 1);  // exclude trailing spaces from the count
  }
  // Fix for Bugzilla 2319 - END

  char buf[SMAX];
  char *bptr = &buf[1];
  const char *sptr = inputData;
  NABoolean isMPLoc = FALSE;
  NABoolean delimited = FALSE;
  SQLCHARSET_CODE isoMappingCS = (SQLCHARSET_CODE)isoMapCS;

  // For SQL/MP tables (NAMETYPE NSK), the user may input a qualified NSK name
  // such as 	\\SYS.$VOL.SUB.TBL  or  $VOL.SUB.TBL.
  // SHORTANSI names can be internally resolved to *fully-qualified* NSK names
  // that is	\\SYS.$VOL.SUB.TBL only.
  // NAMETYPE ANSI follows Ansi rules, disallowing '\\' and '$' in identifiers.
  //
  if (*sptr == '\\') {
    if (setMPLoc()) isMPLoc = TRUE;
  } else if (*sptr == '$') {
    if (setMPLoc()) isMPLoc = TRUE;
  }

  if (!isMPLoc) {
    // begins with invalid char?
    delimited = NOT(NAString2_isRegularStart((unsigned char)*sptr, isoMappingCS));
  }

  size_t len = inputLen;
  for (size_t i = 0; i < len; i++) {
    if (!isMPLoc) {
      if (NOT(NAString2_isRegular((unsigned char)*sptr, isoMappingCS))) {
        delimited = TRUE;
        if (*sptr == '\"') *bptr++ = '\"';  // this will double the dquote"
      }
    }
    *bptr++ = *sptr++;  // (with this line, that is)
  }

  if (delimited) {
    // Verify that trailing blanks were excluded
    sptr--;
    if (isSpace8859_1((unsigned char)*sptr)) {
      ComASSERT(0);    // Note: no-op in Release build.
      *outputLen = 0;  // sorry, this is not supposed to happen
      return;
    }
  } else {
    *bptr = '\0';
    if (!isMPLoc && IsSqlReservedWord(buf + 1)) delimited = TRUE;
  }  // extra checking to determine delimited state is now done

  if (delimited) {
    buf[0] = '\"';   // "
    *bptr++ = '\"';  // "
    *bptr = '\0';
    bptr = &buf[0];
  } else {
    // *bptr = '\0';	//already done above
    bptr = &buf[1];
  }

  len = str_len(bptr);
  if ((len + 1) > outputMaxLen) {
    // sorry, not enough room for the converted identifier
    *outputLen = 0;
    return;
  }

  // copy the converted identifier to the output
  str_cpy_all(outputData, bptr, len);
  outputData[len] = 0;
  *outputLen = len;

}  // ToAnsiIdentifier3

// -----------------------------------------------------------------------
// Remove whitespace (spaces and tabs) from front or back or both
// -----------------------------------------------------------------------
void TrimNAStringSpace(NAString &ns, NABoolean leading, NABoolean trailing) {
  StringPos i;

  if (trailing)
    if (i = ns.length()) {  // assign followed by compare
      for (; i--;)
        if (!isSpace8859_1((unsigned char)ns[i])) break;
      ns.remove(++i);
    }

  if (leading) {
    for (i = 0; i < ns.length(); i++)
      if (!isSpace8859_1((unsigned char)ns[i])) break;
    if (i) ns.remove(0, i);
  }
}

size_t IndexOfFirstWhiteSpace(const NAString &ns, size_t startPos) {
  for (size_t i = startPos; i < ns.length(); i++)
    if (isSpace8859_1((unsigned char)ns[i])) return i;

  return NA_NPOS;
}

size_t IndexOfFirstNonWhiteSpace(const NAString &ns, size_t startPos) {
  for (size_t i = startPos; i < ns.length(); i++)
    if (!isSpace8859_1((unsigned char)ns[i])) return i;

  return NA_NPOS;
}

void RemoveLeadingZeros(NAString &ns) {
  StringPos i;

  for (i = 0; i < (ns.length() - 1); i++)
    if (((unsigned char)ns[i]) != '0') break;
  if (i) ns.remove(0, i);
}

void RemoveTrailingZeros(NAString &ns) {
  StringPos i, strLen = ns.length();
  if (strLen && (((unsigned char)ns[strLen - 1]) == '\0')) {
    i = strLen;
    for (; i--;)
      if (((unsigned char)ns[i]) != '\0') break;

    ns.remove(++i);
  }
}
