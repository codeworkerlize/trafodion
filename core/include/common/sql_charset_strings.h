
#ifndef SQL_CHARSET_STRINGS_H
#define SQL_CHARSET_STRINGS_H

/* -*-C++-*-
******************************************************************************
*
* File:         sql_charset_strings.h
* Description:  Separation of sql charset strings into a separate file so that
*               the same set of strings can be referred to from ComSmallDefs.h
*               without pulling in the rest from SQLCLIdev.h.
*
* Created:      1/7/2003
* Language:     C++
*
*
*
******************************************************************************
*/

#include "common/Platform.h"
// No other includes, please!

#ifndef SQLCHARSETSTRING_DEFINED
#define SQLCHARSETSTRING_DEFINED 1

// Keep in sync with w:/cli/sqlcli.h

#define SQLCHARSETSTRING_ISO88591 "ISO88591"
#define SQLCHARSETSTRING_KANJI    "KANJI"
#define SQLCHARSETSTRING_KSC5601  "KSC5601"
#define SQLCHARSETSTRING_UNICODE  "UCS2"
#define SQLCHARSETSTRING_LATIN    "LATIN"

#define SQLCHARSETSTRING_EUCJP      "EUCJP"
#define SQLCHARSETSTRING_BIG5       "BIG5"
#define SQLCHARSETSTRING_GB18030    "GB18030"
#define SQLCHARSETSTRING_UTF8       "UTF8"
#define SQLCHARSETSTRING_GB2312     "GB2312"
#define SQLCHARSETSTRING_GBK        "GBK"
#define SQLCHARSETSTRING_MB_KSC5601 "MB_KSC5601"
#define SQLCHARSETSTRING_SJIS       "SJIS"

#define SQLCHARSETSTRING_UCS2        SQLCHARSETSTRING_UNICODE
#define SQLCHARSETSTRING_ISO_MAPPING "ISO_MAPPING"

#define SQLCHARSETSTRING_BINARY "BINARY"

#endif  // SQLCHARSETSTRING_DEFINED

#define SQLCHARSETSTRING_ISO88592  "ISO88592"
#define SQLCHARSETSTRING_ISO88593  "ISO88593"
#define SQLCHARSETSTRING_ISO88594  "ISO88594"
#define SQLCHARSETSTRING_ISO88595  "ISO88595"
#define SQLCHARSETSTRING_ISO88596  "ISO88596"
#define SQLCHARSETSTRING_ISO88597  "ISO88597"
#define SQLCHARSETSTRING_ISO88598  "ISO88598"
#define SQLCHARSETSTRING_ISO88599  "ISO88599"
#define SQLCHARSETSYNONYM_SQL_TEXT "SQL_TEXT"
#define SQLCHARSETSTRING_UNKNOWN   "_unknown_"

#define SQLCHARSET_INTRODUCER_IN_LITERAL "_"

#ifndef SQLCOLLATIONSTRING_DEFINED
#define SQLCOLLATIONSTRING_DEFINED 1

// Keep in sync with w:/cli/sqlcli.h

#define SQLCOLLATIONSTRING_DEFAULT  "DEFAULT"
#define SQLCOLLATIONSTRING_CZECH    "CZECH"
#define SQLCOLLATIONSTRING_CZECH_CI "CZECH_CI"
#endif  // SQLCOLLATIONSTRING_DEFINED

#define SQLCOLLATIONSTRING_SJIS    "SJIS"
#define SQLCOLLATIONSTRING_UNKNOWN "_unknown_"

#endif
