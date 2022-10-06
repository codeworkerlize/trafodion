/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ComSqlText.C
 * Description:  methods for class ComSqlTextHandle and the definition
 *               of instance ComSqlText at file scope level
 *
 * Created:      7/31/95
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSqlText.h"

#include <ctype.h>

#include <new>

// -----------------------------------------------------------------------
// file scope definition of ComSqlText
// -----------------------------------------------------------------------

ComSqlTextHandle ComSqlText;

#if 0 /* Not needed since ComSqlTextHandle object has no data members */
UInt32 ComSqlTextHandleInitializer::count_ = 0;

// -----------------------------------------------------------------------
// methods for class ComSqlTextHandleInitializer
// -----------------------------------------------------------------------

ComSqlTextHandleInitializer::ComSqlTextHandleInitializer()
{
  if (count_++)
  {
    return;     // init already done
  }

  new(&ComSqlText) ComSqlTextHandle();
}
#endif

// -----------------------------------------------------------------------
// methods for class ComSqlTextHandle
// -----------------------------------------------------------------------

NABoolean ComSqlTextHandle::isDigit(const char &aChar) const {
  if (isdigit(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isDoubleQuote(const char &aChar) const {
  if (aChar EQU getDoubleQuote()) {
    return TRUE;
  }
  return FALSE;
}

//
// <nondoublequote character> is defined on page 69 of X3H2-93-004
//   For now, this method only supports any <SQL language character>
//   other than a <double quote>
//
NABoolean ComSqlTextHandle::isNonDoubleQuoteChar(const char &aChar) const {
  if (isDoubleQuote(aChar)) {
    return FALSE;
  }
  if (isSqlLangChar(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isIdentifierPart(const char &aChar) const {
  if (isIdentifierStart(aChar) OR isDigit(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isIdentifierPartOrUnderscore(const char &aChar) const {
  if (isIdentifierPart(aChar) OR isUnderscore(aChar)) {
    return TRUE;
  }
  return FALSE;
}

//
// <identifier start> is defined on page 68 of X3H2-93-004
//   For now, this method only supports a <simple Latin letter>
//
NABoolean ComSqlTextHandle::isIdentifierStart(const char &aChar) const {
  if (isSimpleLatinLetter(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isPeriod(const char &aChar) const {
  if (aChar EQU getPeriod()) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSpace(const char &aChar) const {
  if (aChar EQU getSpace()) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSimpleLatinLetter(const char &aChar) const {
  if (isalpha(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSimpleLatinLowerCaseLetter(const char &aChar) const {
  if (isalpha(aChar) AND islower(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSimpleLatinUpperCaseLetter(const char &aChar) const {
  if (isalpha(aChar) AND isupper(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSqlLangChar(const char &aChar) const {
  if (isSimpleLatinLetter(aChar) OR isDigit(aChar) OR isSqlSpecialChar(aChar)) {
    return TRUE;
  }
  return FALSE;
}

NABoolean ComSqlTextHandle::isSqlSpecialChar(const char &aChar) const {
  if (isSpace(aChar)) {
    return TRUE;
  }
  switch (aChar) {
    case '"':   // double quote
    case '%':   // percent
    case '&':   // ampersand
    case '\'':  // quote
    case '(':   // left parenthesis
    case ')':   // right parenthesis
    case '*':   // asterisk
    case '+':   // plus sign
    case ',':   // comma
    case '-':   // minus sign
    case '.':   // period
    case '/':   // solidus
    case ':':   // colon
    case ';':   // semicolon
    case '<':   // less than operator
    case '=':   // equals operator
    case '>':   // greater than operator
    case '?':   // question mark
    case '[':   // left bracket
    case ']':   // right bracket
    case '_':   // underscore
    case '|':   // vertical bar

      return TRUE;

    default:
      return FALSE;

  }  // switch
}

NABoolean ComSqlTextHandle::isUnderscore(const char &aChar) const {
  if (aChar EQU getUnderscore()) {
    return TRUE;
  }
  return FALSE;
}

//
// End of File
//
