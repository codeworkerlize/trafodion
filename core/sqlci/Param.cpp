
#include "sqlci/Param.h"

#include <stdlib.h>
#include <wchar.h>

#include "common/IntervalType.h"
#include "common/NLSConversion.h"
#include "common/dfs2rec.h"
#include "common/nawstring.h"
#include "common/str.h"
#include "exp/exp_clause_derived.h"
#include "sqlci/SqlciCmd.h"
#include "sqlci/SqlciDefs.h"

extern NAHeap sqlci_Heap;

short convDoItMxcs(char *source, int sourceLen, short sourceType, int sourcePrecision, int sourceScale, char *target,
                   int targetLen, short targetType, int targetPrecision, int targetScale, int flags);

Param::Param(char *name_, SetParam *sp_)
    : name(0),
      value(0),
      display_value(0),
      converted_value(0),
      nullValue_(0),
      inSingleByteForm_(TRUE),
      cs(CharInfo::UnknownCharSet),
      isQuotedStrWithoutCharSetPrefix_(FALSE),
      utf16StrLit_(NULL),
      termCS_(CharInfo::UnknownCharSet) {
  if (name_) setName(name_);
  if (sp_) setValue(sp_);
}

Param::Param(char *name_, char *value_, CharInfo::CharSet x_)
    : name(0),
      value(0),
      display_value(0),
      converted_value(0),
      nullValue_(0),
      inSingleByteForm_(TRUE),
      cs(CharInfo::UnknownCharSet),
      isQuotedStrWithoutCharSetPrefix_(FALSE),
      utf16StrLit_(NULL),
      termCS_(CharInfo::UnknownCharSet) {
  if (name_) setName(name_);
  if (value_) setValue(value_, x_);
}

Param::Param(char *name_, NAWchar *value_, CharInfo::CharSet x_)
    : name(0),
      value(0),
      display_value(0),
      converted_value(0),
      nullValue_(0),
      inSingleByteForm_(FALSE),
      cs(CharInfo::UnknownCharSet),
      isQuotedStrWithoutCharSetPrefix_(FALSE),
      utf16StrLit_(NULL),
      termCS_(CharInfo::UnknownCharSet) {
  if (name_) setName(name_);
  if (value_) setValue(value_, x_);
}

Param::~Param() {
  if (name) {
    delete[] name;
    name = 0;
  };

  resetValue_();
}

void Param::setName(const char *name_) {
  if (name) delete[] name;

  name = new char[strlen(name_) + 1];
  strcpy(name, name_);
}

void Param::resetValue_() {
  if (value) {
    delete[] value;
    value = 0;
  }

  if (display_value) {
    delete[] display_value;
    display_value = 0;
  }

  if (converted_value) {
    delete[] converted_value;
    converted_value = 0;
  };

  // it is a non-nullable value
  if (nullValue_) {
    delete[] nullValue_;
    nullValue_ = 0;
  }

  if (utf16StrLit_ != (NAWchar *)NULL) {
    delete[] utf16StrLit_;
    utf16StrLit_ = (NAWchar *)NULL;
  }

  inSingleByteForm_ = TRUE;
  cs = CharInfo::UnknownCharSet;
  isQuotedStrWithoutCharSetPrefix_ = FALSE;
  termCS_ = CharInfo::UnknownCharSet;
}

void Param::setValue_(const char *value_, CharInfo::CharSet x_) {
  cs = x_;
  value = new char[strlen(value_) + 1];
  strcpy(value, value_);
  inSingleByteForm_ = TRUE;
}

void Param::setValue_(const NAWchar *value_, CharInfo::CharSet x_) {
  if (x_ == CharInfo::UNICODE && converted_value) {
    delete[] converted_value;
    converted_value = 0;
  };

  cs = x_;
  NAWchar *wvalue = new NAWchar[NAWstrlen(value_) + 1];
  NAWstrcpy(wvalue, value_);
  value = (char *)wvalue;
  inSingleByteForm_ = FALSE;
}

void Param::setValue(const char *value_, CharInfo::CharSet x_) {
  resetValue_();
  setValue_(value_, x_);
}

void Param::setValue(const NAWchar *value_, CharInfo::CharSet x_) {
  resetValue_();
  setValue_(value_, x_);
}

void Param::setUTF16StrLit(NAWchar *utf16Str) {
  if (utf16StrLit_) delete[] utf16StrLit_;

  utf16StrLit_ = new NAWchar[NAWstrlen(utf16Str) + 1];
  NAWstrcpy(utf16StrLit_, utf16Str);
}

void Param::setValue(SetParam *sp_) {
  resetValue_();
  if (sp_->isInSingleByteForm()) {
    setValue_(sp_->get_argument(), sp_->getCharSet());
  } else {
    setValue_((NAWchar *)(sp_->get_argument()), sp_->getCharSet());
  }
  if (sp_->getUTF16ParamStrLit()) {
    setUTF16StrLit(sp_->getUTF16ParamStrLit());
    isQuotedStrWithoutCharSetPrefix_ = sp_->isQuotedStrWithoutCharSetPrefix();
    setTermCharSet(sp_->getTermCharSet());
  }
}

void Param::makeNull() {
  if (nullValue_) delete[] nullValue_;

  nullValue_ = new char[2];
  *(short *)nullValue_ = -1;
}

short Param::convertValue(SqlciEnv *sqlci_env, short targetType, int &targetLen, int targetPrecision, int targetScale,
                          int vcIndLen, ComDiagsArea *&diags) {
  // get rid of the old converted value
  if (converted_value) {
    delete[] converted_value;
    converted_value = 0;
  };

  short sourceType;
  int sourceLen;

  // set up the source and its length based on the how the value is passed-in.
  if (isInSingleByteForm() == FALSE) {
    sourceLen = (int)(NAWstrlen((NAWchar *)value) * BYTES_PER_NAWCHAR);
    switch (getCharSet()) {
      case CharInfo::UNICODE:
        sourceType = REC_NCHAR_F_UNICODE;
        break;

      case CharInfo::KANJI_MP:
      case CharInfo::KSC5601_MP:
        sourceType = REC_BYTE_F_ASCII;  // KANJI/KSC passed in as NAWchar*
        break;

      default:
        return SQL_Error;  // error case
    }
  } else {
    sourceLen = (int)strlen(value);  // for any source in single-byte format
    sourceType = REC_BYTE_F_ASCII;
  }

  char *pParamValue = value;

  if (DFS2REC::isAnyCharacter(targetType)) {
    if (termCS_ == CharInfo::UnknownCharSet) termCS_ = sqlci_env->getTerminalCharset();
    if (cs == CharInfo::UnknownCharSet) {
      isQuotedStrWithoutCharSetPrefix_ = TRUE;
      cs = termCS_;
    }

    // If the target is CHARACTER and param is set as [_cs_prefix]'...', then
    // make sure the source is assignment compatible with the target.
    CharInfo::CharSet targetCharSet = (CharInfo::CharSet)targetScale;
    if (targetCharSet == CharInfo::UNICODE) {
      if (getUTF16StrLit() == (NAWchar *)NULL) {
        utf16StrLit_ = new NAWchar[sourceLen * 2 + 1];  // plenty of room
        int utf16StrLenInNAWchars =
            LocaleStringToUnicode(cs /*sourceCS*/, /*sourceStr*/ value, sourceLen, utf16StrLit_ /*outputBuf*/,
                                  sourceLen + 1 /*outputBufSizeInNAWchars*/, TRUE /* in - NABoolean addNullAtEnd*/);
        if (sourceLen > 0 && utf16StrLenInNAWchars == 0) return SQL_Error;

        // ComASSERT(utf16StrLenInNAWchars == NAWstrlen(getUTF16StrLit()));
        // Resize the NAWchar buffer to save space
        NAWchar *pNAWcharBuf = new NAWchar[utf16StrLenInNAWchars + 1];
        NAWstrncpy(pNAWcharBuf, utf16StrLit_, utf16StrLenInNAWchars + 1);
        pNAWcharBuf[utf16StrLenInNAWchars] = NAWCHR('\0');  // play it safe
        delete[] utf16StrLit_;
        utf16StrLit_ = pNAWcharBuf;  // do not deallocate pNAWcharBuf
      }
      sourceLen = (int)(NAWstrlen(getUTF16StrLit()) * BYTES_PER_NAWCHAR);
      // check to see if the parameter utf16 string fits in the target
      if (sourceLen > targetLen) return SQL_Error;

      pParamValue = (char *)getUTF16StrLit();
      sourceType = REC_NCHAR_F_UNICODE;
    }

  } else {
    // MP NCHAR (KANJI/KSC) can not be converted to non-character objects
    if (CharInfo::is_NCHAR_MP(cs)) return SQL_Error;
  }

  switch (targetType) {
    case REC_BOOLEAN:
    case REC_BIN8_SIGNED:
    case REC_BIN8_UNSIGNED:
    case REC_BIN16_SIGNED:
    case REC_BIN16_UNSIGNED:
    case REC_BPINT_UNSIGNED:
    case REC_BIN32_SIGNED:
    case REC_BIN32_UNSIGNED:
    case REC_BIN64_SIGNED:
    case REC_BIN64_UNSIGNED:
    case REC_DECIMAL_UNSIGNED:
    case REC_DECIMAL_LSE:
    case REC_FLOAT32:
    case REC_FLOAT64:
    case REC_BYTE_F_ASCII:
    case REC_BYTE_V_ASCII:
    case REC_BYTE_V_ASCII_LONG:
    case REC_NCHAR_F_UNICODE:
    case REC_NCHAR_V_UNICODE:
    case REC_BINARY_STRING:
    case REC_VARBINARY_STRING: {
      char *VCLen = NULL;
      short VCLenSize = 0;
      short origTargetType = 0;

      // 5/27/98: added VARCHAR cases
      if ((targetType == REC_BYTE_V_ASCII) || (targetType == REC_BYTE_V_ASCII_LONG) ||
          (targetType == REC_NCHAR_V_UNICODE) || (targetType == REC_VARBINARY_STRING)) {
        // add bytes for variable length field
        VCLenSize = vcIndLen;  // sizeof(short);
        VCLen = converted_value = new char[targetLen + VCLenSize];
      } else
        converted_value = new char[targetLen];

      ex_expr::exp_return_type ok;
      CharInfo::CharSet TCS = sqlci_env->getTerminalCharset();
      CharInfo::CharSet ISOMAPCS = sqlci_env->getIsoMappingCharset();

      NAString *tempstr;
      if ((DFS2REC::isCharacterString(sourceType) && DFS2REC::isCharacterString(targetType) &&
           !(getUTF16StrLit() != NULL && sourceType == REC_NCHAR_F_UNICODE && targetScale == CharInfo::UCS2) &&
           /*source*/ cs != targetScale /*i.e., targetCharSet*/
           ) &&
          (origTargetType != REC_BLOB)) {
        charBuf cbuf((unsigned char *)pParamValue, sourceLen);
        NAWcharBuf *wcbuf = 0;
        int errorcode = 0;
        wcbuf = csetToUnicode(cbuf, 0, wcbuf, cs /*sourceCharSet*/
                              ,
                              errorcode);
        if (errorcode != 0) return SQL_Error;
        tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), targetScale /*i.e., targetCharSet*/
        );
        if (tempstr == NULL) return SQL_Error;  // Avoid NULL ptr reference if conversion error
        sourceType = targetType;                // we just converted it to the target type
        sourceLen = tempstr->length();
        pParamValue = (char *)tempstr->data();

        if (sourceLen > targetLen) return SQL_Error;
      }

      ok = convDoIt(pParamValue, sourceLen, sourceType,
                    0,            // source Precision
                    targetScale,  // new charset we converted to
                    &converted_value[VCLenSize], targetLen, targetType, targetPrecision, targetScale, VCLen, VCLenSize,
                    &sqlci_Heap, &diags);

      if (ok != ex_expr::EXPR_OK) {
        // No need to delete allocated memory before return because class member
        // converted_value still points to allocated memory that is deleted in
        // desctructor.
        return SQL_Error;  // error case
      }

    }; break;

    case REC_DATETIME: {
      char *VCLen = NULL;
      short VCLenSize = 0;
      converted_value = new char[targetLen + 1];

      UInt32 flags = 0;
      flags |= CONV_NO_HADOOP_DATE_FIX;

      ex_expr::exp_return_type ok = convDoIt(value, sourceLen, sourceType,
                                             0,  // source Precision
                                             0,  // source Scale
                                             converted_value, targetLen, targetType, targetPrecision, targetScale,
                                             VCLen, VCLenSize, &sqlci_Heap, &diags, CONV_UNKNOWN, NULL, flags);

      if (ok != ex_expr::EXPR_OK) {
        return SQL_Error;  // error case
      }
    }; break;

    case REC_INT_YEAR:
    case REC_INT_MONTH:
    case REC_INT_YEAR_MONTH:
    case REC_INT_DAY:
    case REC_INT_HOUR:
    case REC_INT_DAY_HOUR:
    case REC_INT_MINUTE:
    case REC_INT_HOUR_MINUTE:
    case REC_INT_DAY_MINUTE:
    case REC_INT_SECOND:
    case REC_INT_MINUTE_SECOND:
    case REC_INT_HOUR_SECOND:
    case REC_INT_DAY_SECOND: {
      // convert target back to string.
      converted_value = new char[targetLen];
      int convFlags = CONV_ALLOW_SIGN_IN_INTERVAL;
      short ok = convDoItMxcs(value, sourceLen, sourceType,
                              0,  // source Precision
                              0,  // source Scale
                              converted_value, targetLen, targetType, targetPrecision, targetScale, convFlags);

      if (ok != 0) {
        // No need to delete allocated memory before return because class member
        // converted_value still points to allocated memory that is deleted in
        // desctructor.
        return SQL_Error;  // error case
      }
    }; break;

    case REC_NUM_BIG_UNSIGNED:
    case REC_NUM_BIG_SIGNED: {
      converted_value = new char[targetLen];
      if (sourceType == REC_NCHAR_F_UNICODE) {
        ex_expr::exp_return_type ok = convDoIt(pParamValue, sourceLen, sourceType,
                                               0,            // source Precision
                                               targetScale,  // new charset we converted to
                                               &converted_value[0], targetLen, targetType, targetPrecision, targetScale,
                                               NULL, 0, &sqlci_Heap, &diags);
        if (ok != ex_expr::EXPR_OK) {
          return SQL_Error;  // error case
        }
      } else {
        short ok = convDoItMxcs(value, sourceLen, sourceType,
                                0,  // source Precision
                                0,  // source Scale
                                converted_value, targetLen, targetType, targetPrecision, targetScale, 0);
        if (ok != 0) {
          // No need to delete allocated memory before return because class member
          // converted_value still points to allocated memory that is deleted in
          // desctructor.
          return SQL_Error;  // error case
        }
      }

    }; break;

    default:
      break;
  };

  return 0;
}

short Param::contains(const char *value) const {
  if (strcmp(name, value) == 0)
    return -1;
  else
    return 0;
}

char *Param::getDisplayValue(CharInfo::CharSet display_cs) {
  if (isInSingleByteForm() == FALSE && getCharSet() == CharInfo::UNICODE) {
    if (display_value == NULL) {
      NAWchar *wvalue = (NAWchar *)value;
      int wlen = (int)NAWstrlen(wvalue);
      display_value = new char[wlen + 1];
      UnicodeStringToLocale(display_cs, wvalue, wlen, display_value, wlen + 1,
                            TRUE,  // add null at end
                            TRUE   // non-convertable char to ?
      );
    }
    return display_value;

  } else
    return getValue();
}

//////////////////////////////////////////////////////////////
short SetParam::process(SqlciEnv *sqlci_env) { return 0; }

SetParam::~SetParam() {
  if (param_name) delete[] param_name;
  if (m_convUTF16ParamStrLit) delete[] m_convUTF16ParamStrLit;
}

void SetParam::setUTF16ParamStrLit(const NAWchar *utf16Str, size_t ucs2StrLen) {
  if (m_convUTF16ParamStrLit) {
    delete[] m_convUTF16ParamStrLit;
    m_convUTF16ParamStrLit = NULL;
  }
  m_convUTF16ParamStrLit = new NAWchar[ucs2StrLen + 1];
  NAWstrncpy(m_convUTF16ParamStrLit, utf16Str, ucs2StrLen);
  m_convUTF16ParamStrLit[ucs2StrLen] = L'\0';
}

//////////////////////////////////////////////////////////////
short SetPattern::process(SqlciEnv *sqlci_env) { return 0; }

SetPattern::~SetPattern() {
  if (pattern_name) delete[] pattern_name;
}
