
#ifndef PARAM_H
#define PARAM_H

#include "common/Int64.h"
#include "common/NAString.h"
#include "common/charinfo.h"

class SetParam;
class ComDiagsArea;
class Param {
  char *name;
  char *value;
  char *display_value;  // single/multi-byte version of Unicode param value,
                        // for display
  char *converted_value;
  CharInfo::CharSet cs;  // charset of a char-type value. For non-char ones, cs
                         // take the value of CharInfo::UnknownCharSet

  char *nullValue_;             // a two byte buffer containing the null value (-1)
  NABoolean inSingleByteForm_;  // whether the value is in single-byte form.
                                //
                                // Here is how the value is set:
                                //  value passed-in            inSingleByteForm
                                //        char*                       TRUE
                                //        NAWchar*                    FALSE

  NABoolean isQuotedStrWithoutCharSetPrefix_;  // set to TRUE in w:/sqlci/sqlci_yacc.y
                                               // if the parameter value is a string
                                               // literal (i.e., quoted string) AND
                                               // the string literal does not have a
                                               // string literal character set prefix;
                                               // otherwise, this data member is set
                                               // to FALSE.
  NAWchar *utf16StrLit_;                       // When isQuotedStrWithoutCharSetPrefix_ is TRUE,
                                               // this data member points to the UTF16 string
                                               // literal equivalent to the specified quoted
                                               // string parameter; otherwise, this data member
                                               // is set to NULL.
  CharInfo::CharSet termCS_;                   // When isQuotedStrWithoutCharSetPrefix_ is TRUE, this
                                               // data member contains the TERMINAL_CHARSET CQD
                                               // setting at the time the SET PARAM command was
                                               // executed; otherwise, this data member is set to
                                               // CharInfo:UnknownCharSet.

 public:
  Param(char *name_, char *value_, CharInfo::CharSet cs = CharInfo::UnknownCharSet);
  Param(char *name_, NAWchar *value_, CharInfo::CharSet cs = CharInfo::UNICODE);
  Param(char *name_, SetParam *);
  ~Param();

  static NAString getExternalName(const char *name) { return NAString("?") + name; }
  NAString getExternalName() const { return NAString("?") + name; }
  char *getName() const { return name; }
  char *getValue() const { return value; }
  char *getDisplayValue(CharInfo::CharSet = CharInfo::ISO88591);
  // get the display version of the value.
  // returns value data member if non UNICODE params,
  // and returns the converted for Unicode params.
  // The argument is for the desired display charset.
  char *getConvertedValue() { return converted_value; }
  CharInfo::CharSet getCharSet() { return cs; };
  NABoolean isInSingleByteForm() { return inSingleByteForm_; };

  short convertValue(SqlciEnv *, short targetType, int &targetLength, int targetPrecision, int targetScale,
                     int vcIndLen, ComDiagsArea *&diags);
  void setName(const char *name_);

  void setValue(const char *, CharInfo::CharSet cs = CharInfo::UnknownCharSet);
  void setValue(const NAWchar *, CharInfo::CharSet cs = CharInfo::UNICODE);
  void setValue(SetParam *);

  short contains(const char *value) const;

  char *getNullValue() { return nullValue_; }
  short isNull() const { return nullValue_ ? -1 : 0; }
  void makeNull();

  NAWchar *getUTF16StrLit() { return utf16StrLit_; }
  void setUTF16StrLit(NAWchar *utf16Str);

  CharInfo::CharSet getTermCharSet() const { return termCS_; }
  void setTermCharSet(CharInfo::CharSet termCS) { termCS_ = termCS; }

  NABoolean isQuotedStrWithoutCharSetPrefix() const { return isQuotedStrWithoutCharSetPrefix_; }

 protected:
  void resetValue_();  // remove the existing value, and converted_value.
                       // set nullValue_ to 0.

  // helper functions to set the value
  void setValue_(const char *value_, CharInfo::CharSet cs);
  void setValue_(const NAWchar *value_, CharInfo::CharSet cs);
};

#endif
