
#include "common/NAWinNT.h"

#ifndef NDEBUG
#include <assert.h>
#endif

#include <ctype.h>
#include <iostream>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "common/Debug.h"
#include "common/dfs2rec.h"
#include "exp/exp_bignum.h"
#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_interval.h"
#include "exp/exp_datetime.h"
#include "common/DatetimeType.h"
#include "sqlci/Formatter.h"
#include "common/Int64.h"
#include "common/IntervalType.h"
#include "cli/SQLCLIdev.h"
#include "common/str.h"

#include "sqlci/SqlciCmd.h"
#include "common/NLSConversion.h"

short convDoItMxcs(char *source, int sourceLen, short sourceType, int sourcePrecision, int sourceScale,
                   char *target, int targetLen, short targetType, int targetPrecision, int targetScale,
                   int flags);

short convDoItMxcs(char *source, int sourceLen, short sourceType, int sourcePrecision, int sourceScale,
                   char *target, int targetLen, short targetType, int targetPrecision, int targetScale,
                   int flags);

int Formatter::display_length(int datatype, int length, int precision, int scale, int charsetEnum,
                                int heading_len, SqlciEnv *sqlci_env, int *output_buflen) {
  int d_len;
  int d_buflen;

  int scale_len = 0;
  if (scale > 0) scale_len = 1;

  switch (datatype) {
    case REC_BPINT_UNSIGNED:
      // Can set the display size based on precision. For now treat it as
      // unsigned smallint
      d_len = d_buflen = SQL_USMALL_DISPLAY_SIZE;
      break;

    case REC_BIN8_SIGNED:
      d_len = d_buflen = SQL_TINY_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN8_UNSIGNED:
      d_len = d_buflen = SQL_UTINY_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN16_SIGNED:
      d_len = d_buflen = SQL_SMALL_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN16_UNSIGNED:
      d_len = d_buflen = SQL_USMALL_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN32_SIGNED:
      d_len = d_buflen = SQL_INT_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN32_UNSIGNED:
      d_len = d_buflen = SQL_UINT_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN64_SIGNED:
      d_len = d_buflen = SQL_LARGE_DISPLAY_SIZE + scale_len;
      break;

    case REC_BIN64_UNSIGNED:
      d_len = d_buflen = SQL_ULARGE_DISPLAY_SIZE + scale_len;
      break;

    case REC_BYTE_F_ASCII:
      // 12/9/97: added for Unicode case
    case REC_NCHAR_F_UNICODE:
    case REC_NCHAR_V_UNICODE:
    case REC_BYTE_V_ASCII:
    case REC_BYTE_V_ASCII_LONG:
      d_len = length;
      // We usually use 2 ASCII char width for UCS2 characters.
      // We could replace this with the new logic used for UTF-8
      // columns that is character-based, not length-based.
      if ((datatype == REC_NCHAR_F_UNICODE || datatype == REC_NCHAR_V_UNICODE) &&
          sqlci_env->getTerminalCharset() == CharInfo::UTF8)
        d_len /= 2;
      d_buflen =
          CharInfo::getMaxConvertedLenInBytes((CharInfo::CharSet)charsetEnum, length, sqlci_env->getTerminalCharset());
      // we can't have fewer bytes in the buffer than characters in the display
      // (note that we artificially inflate the display len for Unicode when
      // the terminal charset is not UTF-8)
      d_buflen = MAXOF(d_buflen, d_len);
      break;

    case REC_DECIMAL_UNSIGNED:
      d_len = d_buflen = length + scale_len;
      break;

    case REC_DECIMAL_LSE:
      d_len = d_buflen = length + 1 + scale_len;
      break;

    case REC_NUM_BIG_SIGNED: {
      BigNum tmp(length, precision, (short)scale, 0);
      d_len = d_buflen = tmp.getDisplayLength();
    } break;

    case REC_NUM_BIG_UNSIGNED: {
      BigNum tmp(length, precision, (short)scale, -1);
      d_len = d_buflen = tmp.getDisplayLength();
    } break;

    case REC_FLOAT32:
      d_len = d_buflen = SQL_REAL_DISPLAY_SIZE;
      break;

    case REC_FLOAT64:
      d_len = d_buflen = SQL_DOUBLE_PRECISION_DISPLAY_SIZE;
      break;

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
      d_len = d_buflen = ExpInterval::getDisplaySize(datatype, (short)precision, (short)scale);
    } break;

    case REC_DATETIME: {
      d_len = d_buflen = ExpDatetime::getDisplaySize((short)precision, (short)scale);
    } break;

    case REC_BOOLEAN:
      d_len = d_buflen = SQL_BOOLEAN_DISPLAY_SIZE;
      break;

    // binary values are displayed in HEX format.
    // 1 source byte will be displayed as 2 HEX bytes.
    case REC_BINARY_STRING:
    case REC_VARBINARY_STRING:
      d_len = d_buflen = str_computeHexAsciiLen(length);
      break;

    case REC_BLOB:
    case REC_CLOB:
      // TBD perhaps display the hex value len.
      d_len = d_buflen = LOB_HANDLE_LEN;
      break;
    default:
      d_len = d_buflen = length;
      break;
  }

  d_len = MAXOF(d_len, heading_len);
  d_buflen = MAXOF(d_buflen, heading_len);

  if (output_buflen) *output_buflen = d_buflen;

  return d_len;
}

int Formatter::buffer_it(SqlciEnv *sqlci_env, char *data, int datatype, int length, int precision, int scale,
                           char *ind_data, int display_length, int display_buf_length, int null_flag,
                           int vcIndLen, char *buf, int *curpos, NABoolean separatorNeeded,
                           NABoolean checkShowNonPrinting) {
  CharInfo::CharSet tcs = sqlci_env->getTerminalCharset();
  int tcsDataType = CharInfo::getFSTypeFixedChar(tcs);

  // don't support UCS2 as terminal charset, or change code below
  assert(tcsDataType == REC_BYTE_F_ASCII);

  str_pad(buf, display_buf_length, ' ');  // blank pad the buffer

  if (null_flag && ind_data && ind_data[0] == (char)-1) {
    // null value is displayed right justified for numerics and
    // left justified for everything else.
    if ((datatype >= REC_MIN_NUMERIC) && (datatype <= REC_MAX_NUMERIC))
      buf[display_length - 1] = '?';
    else
      buf[0] = '?';
  } else {
    NABoolean ascii = FALSE, varchar = FALSE;

    if ((datatype == REC_BYTE_V_ASCII) || (datatype == REC_BYTE_V_ASCII_LONG) || (datatype == REC_BYTE_V_ANSI) ||
        (datatype == REC_NCHAR_V_UNICODE) ||       // 12/9/97
        (datatype == REC_NCHAR_V_ANSI_UNICODE) ||  // 6/26/98
        (datatype == REC_VARBINARY_STRING) || (datatype == REC_BLOB) || (datatype == REC_CLOB)) {
      varchar = TRUE;

      if (vcIndLen == sizeof(int)) {
        int VCLen;
        str_cpy_all((char *)&VCLen, data, sizeof(int));
        length = (int)VCLen;
        data = &data[sizeof(int)];
      } else {
        short VCLen;
        str_cpy_all((char *)&VCLen, data, sizeof(short));
        length = (int)VCLen;
        data = &data[sizeof(short)];
      }

#ifndef NDEBUG
      static UInt32 asserted = 0;
      if (!asserted++) {
        assert(SQL_VARCHAR_HDR_SIZE == sizeof(short));
      }
#endif
    };

    ULng32 convFlags = CONV_LEFT_PAD;

    switch (datatype) {
      case REC_BYTE_F_ASCII:
      case REC_BYTE_V_ASCII:
      case REC_BYTE_V_ASCII_LONG:
      case REC_BYTE_V_ANSI:
      case REC_BLOB:
      case REC_CLOB: {
        ascii = TRUE;
        convFlags = CONV_ALLOW_INVALID_CODE_VALUE;
      }
        // fall through to the next case

      case REC_BPINT_UNSIGNED:
      case REC_BIN64_SIGNED:
      case REC_BIN64_UNSIGNED:
      case REC_BIN32_SIGNED:
      case REC_BIN32_UNSIGNED:
      case REC_BIN16_SIGNED:
      case REC_BIN16_UNSIGNED:
      case REC_BIN8_SIGNED:
      case REC_BIN8_UNSIGNED:
      case REC_DECIMAL_UNSIGNED:
      case REC_DECIMAL_LS:
      case REC_DECIMAL_LSE:
      case REC_FLOAT32:
      case REC_FLOAT64:
      case REC_DATETIME:
      case REC_BOOLEAN:
        //  case REC_BINARY_STRING:
        //  case REC_VARBINARY_STRING:
        {
          short retcode = convDoIt(data, length, datatype, precision, scale, buf, display_buf_length, tcsDataType,
                                   0,     // targetPrecision
                                   tcs,   // target charset
                                   NULL,  // varCharLen
                                   0,     // varCharLenSize
                                   0,     // heap
                                   0,     // diagsArea
                                   CONV_UNKNOWN, 0, convFlags);

          if (ascii && tcs == CharInfo::UTF8) {
            // we want to display only display_length UTF-8
            // characters, remove any trailing blanks beyond that
            display_length = lightValidateUTF8Str(buf, display_buf_length, display_length, 0);
            if (display_length < 0) {
              UInt32 translatedCharCnt = 0;
              char *firstUntranslatedChar = NULL;
              UInt32 outputDataLen = 0;
              int rc = 0;
              rc = UTF8ToLocale(cnv_version1, data, length, buf, display_buf_length, convertCharsetEnum(tcs),
                                firstUntranslatedChar, &outputDataLen, FALSE, convFlags & CONV_ALLOW_INVALID_CODE_VALUE,
                                &translatedCharCnt, CharInfo::getReplacementCharacter(tcs));
              if (rc < 0)
                display_length = 0;
              else
                display_length = outputDataLen;
            }
          }
        }
        break;

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
      case REC_INT_DAY_SECOND:
      case REC_NUM_BIG_SIGNED:
      case REC_NUM_BIG_UNSIGNED: {
        convFlags |= CONV_LEFT_PAD;
        short retcode = convDoItMxcs(data, length, datatype, precision, scale, buf, display_length, tcsDataType,
                                     0,  // targetPrecision
                                     tcs, convFlags);
      } break;

      case REC_NCHAR_F_UNICODE:  // 12/9/97: added for Unicode case
      case REC_NCHAR_V_UNICODE:
      case REC_NCHAR_V_ANSI_UNICODE: {
        int localeInfo = CharInfo::getTargetCharTypeFromLocale();
        ascii = (localeInfo == REC_SBYTE_LOCALE_F);
        short retcode;
        CharInfo::CharSet TCS = sqlci_env->getTerminalCharset();

        if (TCS != CharInfo::UNICODE) {
          charBuf cbuf((unsigned char *)data, length);
          NAWcharBuf *wcbuf = 0;
          int errorcode = 0;
          wcbuf = csetToUnicode(cbuf, 0, wcbuf, CharInfo::UNICODE, errorcode);
          NAString *tempstr;
          if (errorcode != 0) {
            tempstr = new NAString(" ");
          } else {
            tempstr = unicodeToChar(wcbuf->data(), wcbuf->getStrLen(), TCS, NULL, TRUE);
            TrimNAStringSpace(*tempstr, FALSE, TRUE);  // trim trailing blanks
          }
          length = tempstr->length();
          if (display_buf_length > length) {
            str_cpy_all(buf, tempstr->data(), length);
            /* blank pad target */
            str_pad(&buf[length], display_buf_length - length, ' ');
          } else {
            str_cpy_all(buf, tempstr->data(), display_buf_length);
            if (display_buf_length < length) {
              errorcode = 1;
              retcode = ex_expr::EXPR_ERROR;
            }
          }
          if (TCS == CharInfo::UTF8) {
            // we want to display only display_length UTF-8
            // characters, remove any trailing blanks beyond that
            display_length = lightValidateUTF8Str(buf, display_buf_length, display_length, 0);
            if (display_length < 0) display_length = 0;
          }
          if (errorcode == 0) retcode = ex_expr::EXPR_OK;
        }  // if TCS != CharInfo::UNICODE
        else
          retcode = convDoIt(data, length, datatype, precision, scale, buf, display_buf_length, localeInfo,
                             display_length,  // targetPrecision (max. num chars)
                             0,
                             NULL,  // varCharLen
                             0,     // varCharLenSize
                             NULL,  // heap
                             NULL,  // diags area
                             CONV_UNKNOWN,
                             NULL,  // conversion error flag
                             CONV_ALLOW_INVALID_CODE_VALUE);
      } break;

      case REC_BINARY_STRING:
      case REC_VARBINARY_STRING: {
        int retLen = 0;
        if (length > 0) {
          retLen = str_convertToHexAscii(data, length, buf, display_length, FALSE);
          if (retLen < 0) {
            // should not happen
            assert(0);
          }
        }

        str_pad(&buf[retLen], display_length - retLen, ' ');
      } break;

      default:
        break;
    }

    if (ascii && checkShowNonPrinting) {
      buf[display_length] = '\0';
      display_length += showNonprinting(buf, display_length + 1, varchar);
    }

  }  // !(null_flag && ...), i.e. actual data

  *curpos += display_length;
  if (separatorNeeded) {  // add blanks to separate each output field
    str_pad(&buf[display_length], BLANK_SEP_WIDTH, ' ');
    *curpos += BLANK_SEP_WIDTH;
  }

  return 0;
}

// If env var SQL_MXCI_SHOW_NONPRINTING is reset, or unset, or simply not set,
// or if it's set to the empty string or to '0',
//    no replacement is done -- the standard, default sqlci behavior.
//
// SET DEFINE SQL_MXCI_SHOW_NONPRINTING;  or  export SQL_MXCI_SHOW_NONPRINTING=1
//    we replace each nonprinting char (as defined in the ascii locale)
//    with a 8-bit char you're unlikely to see in other sqlci output,
//    an 8-bit char hardcoded here, specially chosen to be highly visible.
//
// If SQL_MXCI_SHOW_NONPRINTING is set to any non-digit character, say 'X',
//    we replace each nonprinting char with that specified X.
//
// If SQL_MXCI_SHOW_NONPRINTING is set to any other digit
// (other than '0' == no replacement, '1' == highly visible replacement),
//    we replace each nonprinting char with a 3-character hex escape sequence,
//    "\xx".  We return a special value of 9, HEX_EXPANSION_ON,
//    to indicate this setting.
//
//    Caller must provide a large-enough buffer
//    (up to 3 times, i.e. HEX_BUFSIZ_MULTIPLIER times, the nominal size)
//    for this setting to work!
//
// If the second character of what SQL_MXCI_SHOW_NONPRINTING is set to
// is the digit '8' -- e.g.,
//	export SQL_MXCI_SHOW_NONPRINTING=18
//	export SQL_MXCI_SHOW_NONPRINTING='#8'
//    we replace 8-bit chars as well.  The default obviously is to display
//    8-bit chars "as-is" (since they're generally printable/visible).
//
// This is useful when an embedded app inserts fixed CHAR data containing
// embedded NUL characters (or other nonprinting ones) and you want to see
// the data in SQLCI.  For an example, see the embedded regression test
// INS1.sql, function checkKanji().

NABoolean Formatter::replace8bit_ = FALSE;

char Formatter::getShowNonprintingReplacementChar(NABoolean reeval) {
  static NABoolean eval = TRUE;
  static char replacementChar;

  if (reeval) eval = TRUE;
  if (eval) {
    eval = FALSE;
    replacementChar = '\0';
    replace8bit_ = FALSE;
    const char *env = getenv("SQL_MXCI_SHOW_NONPRINTING");
    if (env && *env && *env != '0') {
      if (isdigit(*env)) {
        if (*env == '1') {
          replacementChar = '\xDD';
        } else
          replacementChar = HEX_EXPANSION_ON;
      } else
        replacementChar = *env;

      if (*++env == '8') replace8bit_ = TRUE;
    }
  }

  return replacementChar;

}  // getShowNonprintingReplacementChar()

static inline NABoolean is8bit(int c) { return c < 0 || c > SCHAR_MAX; }

static inline NABoolean isprint8bit(int c) { return is8bit(c) && !Formatter::replace8bit_; }

static inline NABoolean replace(int c) {
  return !(isprint(c) || isspace((unsigned char)c) || isprint8bit(c));
}  //  For VS2003

size_t Formatter::showNonprinting(char *s, size_t z, NABoolean varchar) {
  const char r = getShowNonprintingReplacementChar();
  if (!r) return 0;

  if (z <= 1) {
#ifndef NDEBUG
    if (z == 0 || !varchar) fprintf(stderr, "Error: z==" PFSZ ", varchar==%u\n", z, varchar);
#endif
    if (z == 0) return 0;
  }
  if ((int)z < 0)  // SQL NULL indicator passed in as negative size...
    return 0;

  z--;            // convert from SIZE to OFFSET
  char c = s[z];  // should be the terminal nul byte
  if (c) {
#ifndef NDEBUG
    if (varchar) {
      fprintf(stderr, "Error: nonzero terminating character: s[" PFSZ "]==%d", z, c);
      if (isprint(c)) fprintf(stderr, " (%c)", c);
      fprintf(stderr, "\n");
    }
#endif
    s[z] = '\0';
  }

  // Replacement character of HEX_EXPANSION_ON is special
  size_t nonPrinting = 0;
  size_t zOrig = z;
  while (z--) {
    if (replace(s[z]))
      if (r != HEX_EXPANSION_ON)
        s[z] = r;
      else
        nonPrinting++;
  }
  if (r != HEX_EXPANSION_ON || !nonPrinting) return 0;

#define HEXLEN 2         // byte as 2 hex digits, xx
#define HEXFMT "%02X"    // (the bslash will make it 3)
  char hex[HEXLEN + 1];  // extra byte here for sprintf
#ifndef NDEBUG
  if (HEXLEN + 1 != HEX_BUFSIZ_MULTIPLIER) fprintf(stderr, "Error: HEXxxx %d %d\n", HEXLEN, HEX_BUFSIZ_MULTIPLIER);
#endif

  z = zOrig;
  nonPrinting *= HEXLEN;
  size_t bz = z + nonPrinting;  // OFFSET of terminal nul byte

  // if (bz >= zMaxbuf) {
  //  #ifndef NDEBUG
  //    fprintf(stderr, "Error, overflow: bz==%u, zAltBuf==%u\n", bz, zAltbuf);
  //  #endif
  //  bz = zAltbuf - 1;
  //}

  char *altbuf = s;

  altbuf[bz--] = '\0';
  while (z--) {
    c = s[z];
    if (!replace(c))
      altbuf[bz--] = c;
    else {
      sprintf(hex, HEXFMT, c);
      for (size_t h = HEXLEN; h--;) altbuf[bz--] = hex[h];
      altbuf[bz--] = '\\';  // 3-char escape seq,  \xx
    }
  }
#ifndef NDEBUG
  if (bz != z) fprintf(stderr, "Error: bz==" PFSZ ", z==" PFSZ "\n", bz, z);
#endif

  return nonPrinting;  // the count of EXTRA chars we output

}  // showNonprinting()
