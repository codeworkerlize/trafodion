
#ifndef QUERYTEXT_H
#define QUERYTEXT_H
/* -*-C++-*-
 *****************************************************************************
 * File:         QueryText.h
 * Description:  QueryText encapsulates a SQL statement text that can be a
 *               UCS-2 or ANSI (localized) character string.
 * Created:      7/16/2003
 * Language:     C++
 *****************************************************************************
 */
#include "common/NAWinNT.h"
#include "common/wstr.h"
#include "cli/sqlcli.h"
#include "common/str.h"
#include "common/charinfo.h"

class QueryText {
 public:
  // constructor
  QueryText(char *text, int charset) : text_(text), charset_(charset) {}

  // simple accessors
  int charSet() { return charset_; }
  char *text() { return text_; }
  NAWchar *wText() { return (NAWchar *)text_; }

  // other accessors
  inline int canBeUsedBySqlcompTest(char **text);
  int isNullText() { return !text_; }

  inline int octetLength();
  inline int octetLenPlusOne();
  inline int length();
  NABoolean isDISPLAY() {
    if (!text()) return FALSE;
    if (charSet() == SQLCHARSETCODE_UCS2) {
      NAWchar u[100];
      na_wstr_cpy_convert(u, wText(), 7, -1);
      return na_wcsncmp(u, WIDE_("DISPLAY"), 7) == 0;
    } else {
      char u[100];
      str_cpy_convert(u, text(), 7, -1);
      return str_cmp(u, "DISPLAY", 7) == 0;
    }
  }

  // mutators
  void setText(char *t) { text_ = t; }
  void setCharSet(int cs) { charset_ = cs; }

 private:
  char *text_;  // we don't own this memory
  int charset_;
};

inline int QueryText::canBeUsedBySqlcompTest(char **text) {
  if (charset_ == SQLCHARSETCODE_UCS2 || !text_) {
    return 0;
  } else {
    *text = text_;
    return 1;
  }
}

inline int QueryText::octetLength() {
  return charset_ == SQLCHARSETCODE_UCS2
             ? na_wcslen((const NAWchar *)wText()) * CharInfo::maxBytesPerChar((CharInfo::CharSet)charset_)
             : str_len(text());
}

inline int QueryText::octetLenPlusOne() {
  return octetLength() + CharInfo::maxBytesPerChar((CharInfo::CharSet)charset_);
}

inline int QueryText::length() { return charset_ == SQLCHARSETCODE_UCS2 ? na_wcslen(wText()) : str_len(text()); }

#endif
