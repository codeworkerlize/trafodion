
#pragma once

#if defined(SQLPARSERGLOBALS_FLAGS) || defined(SQLPARSERGLOBALSCMN__INITIALIZE)
#include "common/BaseTypes.h"
#include "SqlParserGlobalsEnum.h"  // to get SqlParser_Flags_Enum

#undef GLOB_
#undef INIT_
#ifdef SQLPARSERGLOBALSCMN__INITIALIZE
#define GLOB_
#define INIT_(val) = val
#else
#define GLOB_ extern
#define INIT_(val)
#endif

GLOB_ THREAD_P int SqlParser_Flags INIT_(0);

inline static int Get_SqlParser_Flags(int flagbits) { return SqlParser_Flags & flagbits; }

// Deprecated; use Or_SqlParser_Flags or the PushAndSetSqlParserFlags
// class instead.
// This method tends to be error-prone because callers often are
// not aware of the different semantics when flagbits are zero.
// Oftentimes this method is coded when the caller really wanted
// to do a simple assign instead.
inline static void Set_SqlParser_Flags(int flagbits) {
  if (flagbits)
    SqlParser_Flags |= flagbits;
  else
    SqlParser_Flags = 0;
}

inline static void Or_SqlParser_Flags(int flagbits) { SqlParser_Flags |= flagbits; }

inline static void Assign_SqlParser_Flags(int flagbits) { SqlParser_Flags = flagbits; }

// Deprecated; use UnOr_SqlParser_Flags or the PushAndSetSqlParserFlags
// class instead.
// This method tends to be error-prone because callers often are
// not aware of the different semantics when flagbits are zero.
inline static void Reset_SqlParser_Flags(int flagbits) {
  if (flagbits)
    SqlParser_Flags &= ~flagbits;
  else
    SqlParser_Flags = 0;
}

// Turns off the bits given in flagbits. If you want to return
// bits to a previous state (whether on or off), use
// Assign_SqlParser_Flags or the PushAndSetSqlParserFlags class.
inline static void UnOr_SqlParser_Flags(int flagbits) { SqlParser_Flags &= ~flagbits; }

class PushAndSetSqlParserFlags {
 public:
  PushAndSetSqlParserFlags(int flagbits) : savedBits_(SqlParser_Flags) { Or_SqlParser_Flags(flagbits); };

  ~PushAndSetSqlParserFlags(void) { Assign_SqlParser_Flags(savedBits_); };

 private:
  int savedBits_;  // the value of SqlParser_Flags at ctor time
};

#endif

#if defined(SQLPARSERGLOBALS_NADEFAULTS) || defined(SQLPARSERGLOBALSCMN__INITIALIZE)
#include "common/charinfo.h"
#include "common/ComMPLoc.h"
#include "sqlcomp/NADefaults.h"
#include "optimizer/ObjectNames.h"

class SqlParser_NADefaults {
 public:
  SqlParser_NADefaults()
      : MPLOC_(),
        MPLOC_as_SchemaName_(),
        NAMETYPE_(DF_ANSI),
        NATIONAL_CHARSET_(CharInfo::UNICODE),
        DEFAULT_CHARSET_(CharInfo::ISO88591),
        ORIG_DEFAULT_CHARSET_(CharInfo::ISO88591),
        ISO_MAPPING_(CharInfo::ISO88591) {}

  ComMPLoc MPLOC_;
  SchemaName MPLOC_as_SchemaName_;
  DefaultToken NAMETYPE_;
  CharInfo::CharSet NATIONAL_CHARSET_;
  CharInfo::CharSet DEFAULT_CHARSET_;
  CharInfo::CharSet ORIG_DEFAULT_CHARSET_;
  CharInfo::CharSet ISO_MAPPING_;
};

#define SqlParser_MPLOC                SqlParser_NADefaults_Glob->MPLOC_
#define SqlParser_MPLOC_as_SchemaName  SqlParser_NADefaults_Glob->MPLOC_as_SchemaName_
#define SqlParser_NAMETYPE             SqlParser_NADefaults_Glob->NAMETYPE_
#define SqlParser_NATIONAL_CHARSET     SqlParser_NADefaults_Glob->NATIONAL_CHARSET_
#define SqlParser_DEFAULT_CHARSET      SqlParser_NADefaults_Glob->DEFAULT_CHARSET_
#define SqlParser_ORIG_DEFAULT_CHARSET SqlParser_NADefaults_Glob->ORIG_DEFAULT_CHARSET_
#define SqlParser_ISO_MAPPING          SqlParser_NADefaults_Glob->ISO_MAPPING_

GLOB_ THREAD_P const SqlParser_NADefaults *SqlParser_NADefaults_Glob INIT_(NULL);

inline NABoolean SqlParser_Initialized() { return SqlParser_NADefaults_Glob != NULL; }

#if defined(SQLPARSERGLOBALS_NADEFAULTS_SET)
// Dangerous -- casting away constness and modifying the global --
// caller must be sure to save and restore!

inline DefaultToken SetSqlParser_NAMETYPE(DefaultToken t) {
  DefaultToken r = SqlParser_NAMETYPE;
  ((SqlParser_NADefaults *)SqlParser_NADefaults_Glob)->NAMETYPE_ = t;
  return r;
}

inline void SetSqlParser_DEFAULT_CHARSET(CharInfo::CharSet cs) {
  ((SqlParser_NADefaults *)SqlParser_NADefaults_Glob)->DEFAULT_CHARSET_ = cs;
}

#endif  // SQLPARSERGLOBALS_NADEFAULTS_SET

#endif  // SQLPARSERGLOBALS_NADEFAULTS

#undef GLOB_
#undef INIT_

