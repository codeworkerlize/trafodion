/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.5.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         sqlciparse
#define yylex           sqlcilex
#define yyerror         sqlcierror
#define yydebug         sqlcidebug
#define yynerrs         sqlcinerrs
#define yylval          sqlcilval
#define yychar          sqlcichar

/* First part of user prologue.  */

#include "common/Platform.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include "common/ComAnsiNamePart.h"
#include "sqlmsg/ParserMsg.h"			// StoreSyntaxError(...ComDiagsArea&...)
#include "sqlci/Sqlci.h"
#include "common/str.h"
#include "common/nawstring.h"
#define   SQLCIPARSEGLOBALS__INITIALIZE
#include "sqlci/SqlciParseGlobals.h"
#include "export/HeapLog.h"
#include "common/charinfo.h"
#include "common/conversionHex.h"
#include "common/ComDistribution.h"
#include "common/ComObjectName.h"
#include "common/ComSchemaName.h"

  extern char **environ;
#define ENVIRON environ
#define PUTENV putenv
  
#define YYDEBUG 1			// Turn on debugging, if needed.
#define YY_LOG_FILE "sqlci.yylog"
#define YYFPRINTF(stderr, format, args...) {FILE* fp=fopen(YY_LOG_FILE, "a+");fprintf(fp, format, ##args);fclose(fp);}

extern NAHeap sqlci_Heap;

  extern "C" {int yylex (void); }

extern ComDiagsArea sqlci_DA;
extern char *sqlcitext;

static char *identifier_name_internal;
static char *identifier_name_internal2;
static int pos_internal;
static int pos_internal2;
static int pos_internal3;
static char *get_stats_str;

//extern "C" { void yyerror(const char *sb); };

static void sqlcierror(const char *)		// (const char *errtext)
{
  // Re-initialize the following variables which are used in sqlci_lex.l
  SqlciParse_SyntaxErrorCleanup = 0;
  SqlciParse_IdentifierExpected = 0;

  if (!SqlciParse_HelpCmd)
    {
      // So syntax errmsg caret points properly, when input is an extra
      // dangling #else		or	#endif --
      //   emit   ^    		not	  ^
      if (SqlciParse_InputPos <= 2)
        if (*SqlciParse_InputStr == '#')
	  SqlciParse_InputPos = 0;
      SqlciError (SQLCI_SYNTAX_ERROR, (ErrorParam *) 0);
      StoreSyntaxError(SqlciParse_InputStr, SqlciParse_InputPos, sqlci_DA, 0);
    }
} // sqlcierror

enum EQuotedStringWithoutPrefixFlag { eNOT_QUOTED_STRING_WITHOUT_CHARSET_PREFIX = 0,
                                      eQUOTED_STRING_WITHOUT_CHARSET_PREFIX = 1 };

static short trimValueText(int/*offset*/ &i,
			   int/*length*/ &len,
			   int/*bool*/   &quoted,
                           CharInfo::CharSet /*charset code*/ &cs,
                           int/* bool*/ &inhexdecimal,
			   int /*bool*/    quoting_allowed = -1,
			   int/*bool*/	  use_i_as_is = 0,
			   int/*bool*/	  cs_allowed = 0,
                           EQuotedStringWithoutPrefixFlag * pQuotedStrWithoutPrefixFlag = NULL
                           , int/*bool*/ keepValueTextAsIs = 0/*false*/
                          )
{
  // Process the value text:
  // remove the trailing semicolon and any squotes (')
  // but leave in any dquotes (").
  //
  // Returns TRUE if syntax error (error in quoted string, basically --
  // which should be already caught by class InputStmt; see its logic
  // around SQLCI_INPUT_MISSING_QUOTE).

  if (pQuotedStrWithoutPrefixFlag != NULL)
    *pQuotedStrWithoutPrefixFlag = eNOT_QUOTED_STRING_WITHOUT_CHARSET_PREFIX;

  quoted = 0;
  cs =  CharInfo::UnknownCharSet;
  inhexdecimal = 0;

  if (!use_i_as_is)
    {
      i = SqlciParse_InputPos - 1;

      // Now the i'th character is a separator char that Lex found,
      // separating the Define name from its value.
      // The following test allows
      //	SET ENVVAR nam;
      //	SET ENVVAR nam ;
      //	SET ENVVAR nam val;
      //	SET ENVVAR nam 'val';
      //	SET ENVVAR nam "val";
      //	SET ENVVAR nam'val';	-- on the analogy of Ansi syntax which
      //	SET ENVVAR nam"val";	-- allows SELECT * FROM tbl"corrName";
      //	SET ENVVAR nam :val;	-- value is  :val
      //	SET ENVVAR nam =val;	-- value is  =val
      // but not
      //	SET ENVVAR nam:val;
      //	SET ENVVAR nam=val;
      //
      if (SqlciParse_OriginalStr[i] != ';'    &&
          SqlciParse_OriginalStr[i] != '\''   &&
	  SqlciParse_OriginalStr[i] != '\"'   &&
	  !isspace(SqlciParse_OriginalStr[i]) &&
	  !isspace(SqlciParse_OriginalStr[i-1]))
	return -1;				// error: space separator needed
    }

  // skip leading blanks
  while (isspace(SqlciParse_OriginalStr[i])) i++;

  // remember the start position of '_' in case we need to go back to position i
  int i_orig = i; 
  NAString origValueText(&SqlciParse_OriginalStr[i_orig]); // for the keepValueTextAsIs case

  // determine if the value is has a valid character set prefix
  if ( cs_allowed && SqlciParse_OriginalStr[i] == '_' )
  {
     int k = i+1;
     char c;
     while ((c=SqlciParse_OriginalStr[k])) {
       if ( c == '\''|| c == ' ' ) // quoted or hexadecimal value
       {
         SqlciParse_OriginalStr[k] = '\0'; // temp. null terminate the prefix
         NAString csPrefix(&SqlciParse_OriginalStr[i+1]);
         SqlciParse_OriginalStr[k] = c; // restore

         csPrefix.toUpper();
         cs = CharInfo::getCharSetEnum(csPrefix); // check the prefix
         if ( CharInfo::isCharSetFullySupported(cs) ) {
            i = k; // mark the start of the string after the prefix
         }  
         break;
       }
       k++;
     }

     // A string in hexadecimal format begins with ' x' or ' X'.
     if ( SqlciParse_OriginalStr[i] == ' ' && 
          SqlciParse_OriginalStr[i+1] &&
          toupper(SqlciParse_OriginalStr[i+1]) == 'X' ) {
        inhexdecimal = 1; // mark the hexadecimal format.
        i += 2;           // skip ' x'. Suppose to point at '. 
     }
  } else {
     // no cs name prefix. Check if it is a hexadecimal string.
     if ( toupper(SqlciParse_OriginalStr[i]) == 'X' &&
          SqlciParse_OriginalStr[i+1] == '\''
        ) {
        inhexdecimal = 1; // mark the hexadecimal format.
        i += 1;           // skip 'x'. Suppose to point at '. 
     }
  }

  char delimQuote = '\'';
  if (SqlciParse_OriginalStr[i] == '\'' ||	// quoted value
      SqlciParse_OriginalStr[i] == '"')
    {
      delimQuote = SqlciParse_OriginalStr[i];
      quoted = -1;

      // Remove the leading single quote as procedure comments indicate. 
      NAString temptrail(&SqlciParse_OriginalStr[i+1]);
      strcpy(&SqlciParse_OriginalStr[i], temptrail);
    } else
       i = i_orig; // need to abadone the charset name assumption.
    
  SqlciParse_InputPos = i;			// for sqlcierror message
  if (quoted && !quoting_allowed) return -1;	// error

  int j = strlen(SqlciParse_OriginalStr) - 1;

  // skip any blanks between end of SqlciParse_OriginalStr and the semicolon.
  while (isspace(SqlciParse_OriginalStr[j])) j--;

  j--;						// skip semicolon

  while (j >= i &&				// skip trailing blanks
	 isspace(SqlciParse_OriginalStr[j])) j--;

  if (j < i && quoted) return -1;		// error: unmatched begin quote

  if (SqlciParse_OriginalStr[j] == delimQuote)
    {
      if (!quoted) return -1;			// error: unmatched end quote
      quoted = +1;
      j--;
    }
  if (quoted < 0) return -1;			// error: unmatched begin quote

  if (j >= i)
    if (quoted)
      {
	// if nonempty string within quotes, convert any embedded pair of quotes
	// (i.e. a quoted quote) into one quote, by left shifting
	//
	// **this modifies SqlciParse_OriginalStr (which points to the same
	// **buffer as SqlciParse_InputStr)!

	//#ifdef NA_WINNT
   SqlciParse_OriginalStr[j+1] = '\0';	// mark end of value text
   //#else
   // Remove the trailing single quote, but save the semi-colon on the TANDEM
   // platform.
   //   SqlciParse_OriginalStr[j+1] = '\;';	// replace quote with a semi-colon
   //   SqlciParse_OriginalStr[j+2] = '\0';	// mark end of string
   //#endif // NA_WINNT

	for (j = i; SqlciParse_OriginalStr[j]; j++)
	  if (SqlciParse_OriginalStr[j] == delimQuote)
	    {
	      if (SqlciParse_OriginalStr[j+1] != delimQuote)
		return -1;			// error: unmatched embedded q
                
              NAString temptrail(&SqlciParse_OriginalStr[j+1]);
 
	      strcpy(&SqlciParse_OriginalStr[j], temptrail);
	    }
	  j--;
      }
    else if (quoting_allowed)
      {
        // if nonempty nonquoted string -- but quoting allowed --
	// then any embedded quotes or blanks are an error
	for (int k = i; k <= j; k++)
	  if (SqlciParse_OriginalStr[k] == delimQuote ||
	      isspace(SqlciParse_OriginalStr[k]))
	    {
	      SqlciParse_InputPos = j;		// for sqlcierror message
	      return -1;			// error
	    }
      }
    //else embedded quotes allowed and passed thru unchanged

  if (keepValueTextAsIs)
  {
    TrimNAStringSpace(origValueText, 0/*false*/ /*leading?*/, 1/*true*/ /*trailing?*/);
    if (origValueText.data()[origValueText.length()-1] == ';')
      origValueText.remove(origValueText.length()-1);
    TrimNAStringSpace(origValueText, 0/*false*/ /*leading?*/, 1/*true*/ /*trailing?*/);
    strcpy(&SqlciParse_OriginalStr[i_orig], origValueText.data());
    len = origValueText.length();
  }
  else
    len = j - i + 1;
  if (len < 0) len = 0;
 
  if ( quoted ) {
    if ( cs == CharInfo::UnknownCharSet ) {
      if (pQuotedStrWithoutPrefixFlag != NULL)
        *pQuotedStrWithoutPrefixFlag = eQUOTED_STRING_WITHOUT_CHARSET_PREFIX;
      cs = SqlciEnvGlobal->getTerminalCharset();
    }

  } else 
    inhexdecimal = 0; // reset the hex indicator if the value is not quoted.

  // Consumed entire input (need this for pre-YYACCEPT test in sqlci_cmd).
  // Of course, a subsequent call to this routine on the same string will
  // have to use the use_i_as_is parameter and pass in a valid i ...
  //
  SqlciParse_InputPos = strlen(SqlciParse_OriginalStr) + 1;

  return 0;					// no error

} // trimValueText

static char * FCString (const char *idString, int isFC)
{
  char * cmd_str = new char[strlen(SqlciParse_OriginalStr)];
  cmd_str[0]='\0';

  // Skip leading blanks
  int i = 0;
  while (isspace(SqlciParse_OriginalStr[i]))
    i++;

  const char *s;  
  if (isFC)
    // i+2 - to skip the two characters of 'fc'
    s = strstr(&SqlciParse_OriginalStr[i+2], idString);
  else 
    s = strstr(&SqlciParse_OriginalStr[i], idString);
  
  if (s)
  {
    for ( ; *s; s++)
    {
  	  if (*s != ';')
      {
	     strncat (cmd_str, s, 1);
      }
    }
  }

  return cmd_str;
}

#define REPOSITION_SqlciParse_InputPos \
  	  { SqlciParse_InputPos = strlen(SqlciParse_InputStr); }



# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
#ifndef YY_SQLCI_HOME_ESOYE_TRAFODION_INCLUDECORE_SQLCI_SQLCI_YACC_H_INCLUDED
# define YY_SQLCI_HOME_ESOYE_TRAFODION_INCLUDECORE_SQLCI_SQLCI_YACC_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int sqlcidebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    NOSQL = 258,
    CONTAINSQL = 259,
    READSQL = 260,
    MODIFYSQL = 261,
    RESETVIOLATION = 262,
    CHECKVIOLATION = 263,
    NoAutoXact = 264,
    DESCRIBEToken = 265,
    CREATECONTEXT = 266,
    CURRENTCONTEXT = 267,
    SWITCHCONTEXT = 268,
    DELETECONTEXT = 269,
    RESETCONTEXT = 270,
    ADDtoken = 271,
    ALLtoken = 272,
    BACKSLASH = 273,
    BRIEF = 274,
    DETAIL = 275,
    CACHE = 276,
    CANCEL = 277,
    CLASStoken = 278,
    CLEANUP = 279,
    CLEAR = 280,
    COMMA = 281,
    COMMANDStoken = 282,
    CONTROL = 283,
    OSIM = 284,
    COPY = 285,
    CPU_VALUE = 286,
    ERRORtoken = 287,
    SUBERRORtoken = 288,
    HBASEtoken = 289,
    TMtoken = 290,
    DEFAULTtoken = 291,
    DEFAULT_CHARSETtoken = 292,
    DESC = 293,
    DESCRIBE = 294,
    DISPLAY = 295,
    DISPLAY_STATISTICS = 296,
    DISPLAY_QUERYID = 297,
    DISPLAY_EXPLAIN = 298,
    DISPLAY_QC = 299,
    DISPLAY_QC_ENTRIES = 300,
    DISPLAY_QC_ENTRIES_NOTIME = 301,
    DISPLAY_USE_OF = 302,
    EDIT = 303,
    MXCI_TOK_ENV = 304,
    EXIT = 305,
    EXPLAIN = 306,
    FIRST = 307,
    FC = 308,
    FILEtoken = 309,
    FILEINFO = 310,
    FILENAMES = 311,
    FILES = 312,
    GENERATEtoken = 313,
    MERGEtoken = 314,
    METADATAtoken = 315,
    REPLICATEtoken = 316,
    GETtoken = 317,
    GETSTATISTICStoken = 318,
    HISTORY = 319,
    HYPHEN = 320,
    INFER_CHARSET = 321,
    INFOtoken = 322,
    INtoken = 323,
    INFILE = 324,
    SHOWENV = 325,
    SHOWSTATS = 326,
    SHOWTRANSACTION = 327,
    INVOKE = 328,
    LISTCOUNT = 329,
    LISTMODULES = 330,
    LOADtoken = 331,
    EXTRACTtoken = 332,
    LOCK = 333,
    LOCKINGtoken = 334,
    LPAREN = 335,
    MAPtoken = 336,
    MIGRATE = 337,
    MODE = 338,
    MODIFY = 339,
    MODIFYV = 340,
    MSCKtoken = 341,
    NEXT = 342,
    NOEtoken = 343,
    OBEY = 344,
    OBJECTtoken = 345,
    OFtoken = 346,
    OFF = 347,
    ON = 348,
    OPTIONStoken = 349,
    ISO_MAPPING = 350,
    OUTFILE = 351,
    PARAM = 352,
    PARSERFLAGS = 353,
    SQ_LINUX_PATTERN = 354,
    PATTERN_AS_IS = 355,
    PID_VALUE = 356,
    PROCEDUREtoken = 357,
    PURGEDATA = 358,
    POPULATE = 359,
    VALIDATEtoken = 360,
    QUIESCE = 361,
    REFRESH = 362,
    REPEAT = 363,
    REPORT = 364,
    RESET = 365,
    RESULT = 366,
    RPAREN = 367,
    SETtoken = 368,
    SETENV = 369,
    SHOW = 370,
    SHOWCONTROL = 371,
    SHOWINFO = 372,
    SHOWDDL = 373,
    SHOWPLAN = 374,
    SHOWSHAPE = 375,
    SHOWSET = 376,
    SLEEPtoken = 377,
    SQL = 378,
    SQLINFO = 379,
    SQLNAMES = 380,
    STATEMENTtoken = 381,
    STOREtoken = 382,
    SYNTAX = 383,
    SYSTEMtoken = 384,
    EXAMPLE = 385,
    STATISTICS = 386,
    SET_TABLEtoken = 387,
    SET_TRANSACTIONtoken = 388,
    TERMINAL_CHARSET = 389,
    TRANSFORM = 390,
    TRUNCATE = 391,
    WITH = 392,
    UNLOCK = 393,
    UPD_STATS = 394,
    UPD_HIST_STATS = 395,
    USERtoken = 396,
    TENANTtoken = 397,
    USING = 398,
    TABLE = 399,
    VALUES = 400,
    VALIDATE = 401,
    VERBOSE = 402,
    VERIFY = 403,
    VERSIONtoken = 404,
    WAITtoken = 405,
    DEFINEtoken = 406,
    ENVVARtoken = 407,
    PREPARED = 408,
    SESSIONtoken = 409,
    LOG = 410,
    LS = 411,
    CD = 412,
    SH = 413,
    SHELL = 414,
    SELECTtoken = 415,
    UPDATEtoken = 416,
    INSERTtoken = 417,
    DELETEtoken = 418,
    UPSERTtoken = 419,
    ROWSETtoken = 420,
    REPOSITORYtoken = 421,
    CREATE = 422,
    ALTER = 423,
    DROP = 424,
    PREPAREtoken = 425,
    EXECUTEtoken = 426,
    FROM = 427,
    DECLAREtoken = 428,
    OPENtoken = 429,
    CLOSEtoken = 430,
    FETCHtoken = 431,
    DEALLOCtoken = 432,
    CURSORtoken = 433,
    FORtoken = 434,
    CPU = 435,
    PID = 436,
    QID = 437,
    ACTIVEtoken = 438,
    ACCUMULATEDtoken = 439,
    PERTABLEtoken = 440,
    PROGRESStoken = 441,
    TOK_BEGIN = 442,
    COMMIT = 443,
    ROLLBACK = 444,
    WORK = 445,
    SQLCI_CMD = 446,
    SQL_STMT = 447,
    UTIL_STMT = 448,
    EXIT_STMT = 449,
    ERROR_STMT = 450,
    SHELL_CMD = 451,
    IDENTIFIER = 452,
    NUMBER = 453,
    PARAM_NAME = 454,
    PATTERN_NAME = 455,
    FILENAME = 456,
    QUOTED_STRING = 457,
    DQUOTED_STRING = 458,
    GRANTtoken = 459,
    REVOKEtoken = 460,
    REGISTER = 461,
    UNREGISTER = 462,
    GIVE = 463,
    INITIALIZE = 464,
    REINITIALIZE = 465,
    CATALOG = 466,
    SCHEMA = 467,
    HIVEtoken = 468,
    PROCESStoken = 469,
    IF = 470,
    WHILE = 471,
    MPLOC = 472,
    NAMETYPE = 473,
    SIGNAL = 474,
    UIDtoken = 475,
    BACKUP = 476,
    BACKUPS = 477,
    RESTORE = 478,
    DUMP = 479,
    EXPORTtoken = 480,
    IMPORTtoken = 481,
    CURSORWITH = 482,
    WITHOUT = 483,
    HOLD = 484,
    INTERNAL = 485,
    MVLOG = 486,
    UNLOAD = 487,
    CALLToken = 488,
    COMMENTtoken = 489,
    CHECK = 490,
    TOK_SAVEPOINT = 491,
    TOK_RELEASE = 492
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{

         SqlciCmd * sqlci_cmd_type;
         SqlciNode * sql_cmd_type;
         int intval_type;
	 char * stringval_type;
	 dml_type dml_stmt_type;
	 Reset::reset_type reset_stmt_type_;
         SqlciCursorInfo *cursor_info_type_;
       


};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE sqlcilval;

int sqlciparse (void);

#endif /* !YY_SQLCI_HOME_ESOYE_TRAFODION_INCLUDECORE_SQLCI_SQLCI_YACC_H_INCLUDED  */



#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))

/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                            \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  151
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   689

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  238
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  37
/* YYNRULES -- Number of rules.  */
#define YYNRULES  202
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  274

#define YYUNDEFTOK  2
#define YYMAXUTOK   492


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155,   156,   157,   158,   159,   160,   161,   162,   163,   164,
     165,   166,   167,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,   178,   179,   180,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,   206,   207,   208,   209,   210,   211,   212,   213,   214,
     215,   216,   217,   218,   219,   220,   221,   222,   223,   224,
     225,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   568,   568,   589,   599,   606,   605,   614,   620,   619,
     629,   635,   642,   646,   650,   653,   656,   660,   664,   818,
     828,   846,   851,   857,   856,   866,   871,   878,   884,   890,
     896,   903,   909,   920,   924,   929,   934,   939,   944,   949,
     955,   959,   963,   979,   984,   993,   994,   997,  1002,  1008,
    1009,  1010,  1014,  1015,  1016,  1017,  1018,  1022,  1030,  1040,
    1047,  1054,  1046,  1069,  1076,  1068,  1101,  1112,  1123,  1134,
    1146,  1153,  1145,  1163,  1172,  1189,  1188,  1206,  1211,  1242,
    1267,  1290,  1301,  1321,  1325,  1320,  1340,  1347,  1339,  1365,
    1369,  1373,  1364,  1389,  1396,  1388,  1413,  1412,  1445,  1444,
    1454,  1460,  1466,  1475,  1485,  1490,  1495,  1502,  1514,  1518,
    1523,  1535,  1534,  1556,  1557,  1558,  1559,  1560,  1561,  1562,
    1563,  1564,  1565,  1566,  1567,  1568,  1569,  1570,  1571,  1572,
    1573,  1574,  1575,  1576,  1577,  1578,  1579,  1580,  1581,  1582,
    1583,  1584,  1585,  1586,  1587,  1588,  1589,  1590,  1591,  1592,
    1593,  1594,  1595,  1596,  1597,  1598,  1599,  1600,  1601,  1602,
    1603,  1604,  1605,  1606,  1607,  1608,  1609,  1610,  1611,  1612,
    1613,  1614,  1615,  1616,  1617,  1618,  1619,  1620,  1621,  1622,
    1623,  1624,  1625,  1626,  1627,  1628,  1629,  1630,  1631,  1632,
    1633,  1634,  1635,  1636,  1637,  1641,  1642,  1643,  1644,  1645,
    1646,  1651,  1655
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 0
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NOSQL", "CONTAINSQL", "READSQL",
  "MODIFYSQL", "RESETVIOLATION", "CHECKVIOLATION", "NoAutoXact",
  "DESCRIBEToken", "CREATECONTEXT", "CURRENTCONTEXT", "SWITCHCONTEXT",
  "DELETECONTEXT", "RESETCONTEXT", "ADDtoken", "ALLtoken", "BACKSLASH",
  "BRIEF", "DETAIL", "CACHE", "CANCEL", "CLASStoken", "CLEANUP", "CLEAR",
  "COMMA", "COMMANDStoken", "CONTROL", "OSIM", "COPY", "CPU_VALUE",
  "ERRORtoken", "SUBERRORtoken", "HBASEtoken", "TMtoken", "DEFAULTtoken",
  "DEFAULT_CHARSETtoken", "DESC", "DESCRIBE", "DISPLAY",
  "DISPLAY_STATISTICS", "DISPLAY_QUERYID", "DISPLAY_EXPLAIN", "DISPLAY_QC",
  "DISPLAY_QC_ENTRIES", "DISPLAY_QC_ENTRIES_NOTIME", "DISPLAY_USE_OF",
  "EDIT", "MXCI_TOK_ENV", "EXIT", "EXPLAIN", "FIRST", "FC", "FILEtoken",
  "FILEINFO", "FILENAMES", "FILES", "GENERATEtoken", "MERGEtoken",
  "METADATAtoken", "REPLICATEtoken", "GETtoken", "GETSTATISTICStoken",
  "HISTORY", "HYPHEN", "INFER_CHARSET", "INFOtoken", "INtoken", "INFILE",
  "SHOWENV", "SHOWSTATS", "SHOWTRANSACTION", "INVOKE", "LISTCOUNT",
  "LISTMODULES", "LOADtoken", "EXTRACTtoken", "LOCK", "LOCKINGtoken",
  "LPAREN", "MAPtoken", "MIGRATE", "MODE", "MODIFY", "MODIFYV",
  "MSCKtoken", "NEXT", "NOEtoken", "OBEY", "OBJECTtoken", "OFtoken", "OFF",
  "ON", "OPTIONStoken", "ISO_MAPPING", "OUTFILE", "PARAM", "PARSERFLAGS",
  "SQ_LINUX_PATTERN", "PATTERN_AS_IS", "PID_VALUE", "PROCEDUREtoken",
  "PURGEDATA", "POPULATE", "VALIDATEtoken", "QUIESCE", "REFRESH", "REPEAT",
  "REPORT", "RESET", "RESULT", "RPAREN", "SETtoken", "SETENV", "SHOW",
  "SHOWCONTROL", "SHOWINFO", "SHOWDDL", "SHOWPLAN", "SHOWSHAPE", "SHOWSET",
  "SLEEPtoken", "SQL", "SQLINFO", "SQLNAMES", "STATEMENTtoken",
  "STOREtoken", "SYNTAX", "SYSTEMtoken", "EXAMPLE", "STATISTICS",
  "SET_TABLEtoken", "SET_TRANSACTIONtoken", "TERMINAL_CHARSET",
  "TRANSFORM", "TRUNCATE", "WITH", "UNLOCK", "UPD_STATS", "UPD_HIST_STATS",
  "USERtoken", "TENANTtoken", "USING", "TABLE", "VALUES", "VALIDATE",
  "VERBOSE", "VERIFY", "VERSIONtoken", "WAITtoken", "DEFINEtoken",
  "ENVVARtoken", "PREPARED", "SESSIONtoken", "LOG", "LS", "CD", "SH",
  "SHELL", "SELECTtoken", "UPDATEtoken", "INSERTtoken", "DELETEtoken",
  "UPSERTtoken", "ROWSETtoken", "REPOSITORYtoken", "CREATE", "ALTER",
  "DROP", "PREPAREtoken", "EXECUTEtoken", "FROM", "DECLAREtoken",
  "OPENtoken", "CLOSEtoken", "FETCHtoken", "DEALLOCtoken", "CURSORtoken",
  "FORtoken", "CPU", "PID", "QID", "ACTIVEtoken", "ACCUMULATEDtoken",
  "PERTABLEtoken", "PROGRESStoken", "TOK_BEGIN", "COMMIT", "ROLLBACK",
  "WORK", "SQLCI_CMD", "SQL_STMT", "UTIL_STMT", "EXIT_STMT", "ERROR_STMT",
  "SHELL_CMD", "IDENTIFIER", "NUMBER", "PARAM_NAME", "PATTERN_NAME",
  "FILENAME", "QUOTED_STRING", "DQUOTED_STRING", "GRANTtoken",
  "REVOKEtoken", "REGISTER", "UNREGISTER", "GIVE", "INITIALIZE",
  "REINITIALIZE", "CATALOG", "SCHEMA", "HIVEtoken", "PROCESStoken", "IF",
  "WHILE", "MPLOC", "NAMETYPE", "SIGNAL", "UIDtoken", "BACKUP", "BACKUPS",
  "RESTORE", "DUMP", "EXPORTtoken", "IMPORTtoken", "CURSORWITH", "WITHOUT",
  "HOLD", "INTERNAL", "MVLOG", "UNLOAD", "CALLToken", "COMMENTtoken",
  "CHECK", "TOK_SAVEPOINT", "TOK_RELEASE", "$accept", "statement",
  "sqlci_cmd", "$@1", "$@2", "$@3", "commands_only", "empty",
  "reset_type_", "stats_active_clause", "stats_merge_clause",
  "qid_identifier", "sql_cmd", "$@4", "$@5", "$@6", "$@7", "$@8", "$@9",
  "$@10", "$@11", "$@12", "$@13", "$@14", "$@15", "$@16", "$@17", "$@18",
  "$@19", "$@20", "$@21", "optional_rs_index", "cursor_info", "$@22",
  "dml_type", "dml_simple_table_type", "explain_options", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_int16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,   411,   412,   413,   414,
     415,   416,   417,   418,   419,   420,   421,   422,   423,   424,
     425,   426,   427,   428,   429,   430,   431,   432,   433,   434,
     435,   436,   437,   438,   439,   440,   441,   442,   443,   444,
     445,   446,   447,   448,   449,   450,   451,   452,   453,   454,
     455,   456,   457,   458,   459,   460,   461,   462,   463,   464,
     465,   466,   467,   468,   469,   470,   471,   472,   473,   474,
     475,   476,   477,   478,   479,   480,   481,   482,   483,   484,
     485,   486,   487,   488,   489,   490,   491,   492
};
# endif

#define YYPACT_NINF (-224)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-76)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
       5,   -91,  -224,  -224,  -224,  -179,   -13,  -224,  -224,  -224,
    -165,   -53,  -224,  -224,  -224,  -224,  -224,  -224,  -167,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,   -74,   -66,
    -224,  -224,  -224,  -224,  -224,  -224,   -48,   471,   -19,  -224,
    -224,  -224,  -224,  -224,  -224,  -144,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -161,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -139,  -128,  -125,
    -124,  -123,  -118,  -117,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,    87,  -224,  -224,  -224,  -224,  -105,    64,  -104,  -101,
     412,  -120,  -224,  -109,  -224,   223,  -224,  -224,  -224,   -97,
     -95,   -96,  -224,   -90,   -69,   -86,   -85,   -82,   -87,   -83,
     -62,     3,   -67,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,   107,   107,  -224,    -8,   -42,  -224,  -224,  -224,
    -224,  -224,  -224,   -10,  -224,  -224,  -224,  -224,  -112,   118,
    -224,   -27,  -100,  -185,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,   -55,  -224,  -224,  -224,  -224,  -224,   121,  -224,  -224,
    -224,  -224,   148,  -224,   153,   -18,    13,   -70,   -14,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
      97,   158,   159,  -224,  -224,   -44,     7,    -6,    -6,    -6,
     161,   -23,    94,    60,  -224,  -224,   412,    16,  -224,  -224,
      -2,  -224,  -224,  -224,  -224,    -1,  -224,  -224,  -224,  -224,
    -224,  -224,    -3,   173,  -224,  -224,    72,  -224,   -23,   -23,
     -23,  -224,  -224,   106,   -74,  -224,  -224,  -224,  -224,  -224,
    -224,    -6,     1,  -224,    90,  -224,  -224,    89,  -224,  -224,
     -23,     6,  -224,  -224
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,   152,   130,   114,   115,     0,     0,   155,   156,    98,
      81,    47,    12,   188,   187,   127,   134,   186,   100,   101,
     149,   153,   154,   157,   183,   185,   162,   199,     0,     0,
     169,   184,   175,   178,   179,   180,     0,     0,   147,   148,
     150,   151,   159,   160,   161,     0,   167,   168,   181,   176,
     177,   163,   132,   133,   196,   197,    43,    11,   195,   122,
     123,   126,   124,   125,   128,   129,   131,     0,    77,     0,
       0,     0,     0,     0,   164,   165,   166,   138,   139,   140,
     141,   137,   135,   136,   191,   170,   171,   172,   142,   143,
     144,   145,   146,   173,   174,   200,   182,   192,   190,   193,
     194,     0,     2,     3,    59,   113,     0,    13,     0,     0,
       0,     0,    82,     0,   201,     0,   102,   198,     4,    48,
     106,     0,    42,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   121,   116,   117,   118,   119,   120,   158,
     103,    44,    47,    47,    70,    74,    60,    66,    68,    67,
      69,     1,    73,     0,    16,    17,   152,   189,     0,   147,
      99,     0,     0,     0,   202,    79,    78,    80,    40,   105,
      41,    35,    39,    38,    37,    36,    34,    18,   104,    19,
      20,     0,    22,    21,    30,    29,    25,    26,    27,    28,
      33,    45,     5,    46,     8,     0,     0,     0,     0,    14,
      15,   107,    86,    83,    93,    89,    57,    58,    96,    23,
       0,     0,     0,    71,    76,     0,     0,    47,    47,    47,
       0,    47,     0,     0,     6,     9,     0,     0,    64,    50,
       0,    51,    87,    84,    94,     0,    55,    52,    53,    54,
      56,    97,     0,    31,    72,    61,     0,    49,    47,    47,
      47,    90,    24,     0,     0,   111,    65,   110,    88,    85,
      95,    47,     0,    62,    47,    91,    32,     0,   108,   112,
      47,     0,    92,   109
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -224,  -224,  -224,  -224,  -224,  -224,    62,   -11,  -224,  -202,
    -223,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,  -224,
    -224,  -224,  -224,  -224,  -107,     0,  -224
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,   101,   102,   211,   212,   222,   192,   240,   122,   232,
     241,   208,   103,   197,   254,   198,   246,   195,   226,   196,
     218,   249,   217,   248,   220,   261,   270,   219,   250,   221,
     110,   269,   256,   264,   104,   105,   115
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
     114,   204,   139,   160,   202,    27,    28,   181,   167,   199,
     200,   229,   206,   236,   111,     1,   233,   234,   207,   107,
     184,   108,   109,   172,   173,   258,   259,   260,   117,     2,
     182,   183,   112,     3,     4,   106,   142,     5,     6,   185,
     143,   113,   133,     7,     8,     9,    10,   272,    11,   119,
     120,   121,   201,   116,   141,    12,    13,   118,   144,   265,
     161,   162,   163,    14,    15,    16,    17,    18,    19,   145,
      54,    55,   146,   147,   148,    20,    21,    22,    23,   149,
     150,    24,    25,    26,    27,    28,    58,   151,    29,    30,
     153,    31,   152,   164,   154,   186,   187,   155,   205,   134,
     135,   136,   168,   169,   170,   137,   138,   171,    32,    33,
      34,   176,    35,   179,   177,    36,   178,   180,    37,   244,
      38,    39,    40,    41,    42,    43,    44,    45,   174,   175,
     190,   193,   193,   140,   191,   -75,   -63,    46,    47,   139,
      48,    49,    50,    51,    52,    53,   209,   210,    -7,    54,
      55,    27,    28,   -10,   213,    56,   214,   215,    95,   223,
      57,   237,   238,   239,   216,    58,    59,    60,    61,    62,
      63,   203,    64,    65,    66,    67,    68,   230,    69,    70,
      71,    72,    73,   224,   225,   227,   228,   235,   188,   189,
     242,   243,    74,    75,    76,   245,   247,   251,   252,   253,
     262,   267,   271,   266,   273,   194,   231,   231,   231,    77,
      78,    79,    80,    81,    82,    83,    54,    55,     0,    84,
      85,    86,     0,     0,    87,     0,    88,     0,    89,    90,
      91,    92,    58,   156,     0,    93,    94,    95,    96,    97,
      98,    99,   100,     0,     0,     0,   257,     2,     0,     0,
     231,     3,     4,   268,   263,     0,     0,     0,     0,     0,
       0,     7,     8,     0,     0,     0,     0,     0,     0,   255,
       0,     0,     0,     0,    13,     0,     0,     0,     0,     0,
       0,    14,    15,    16,    17,   157,     0,     0,     0,     0,
       0,     0,     0,    20,    21,    22,    23,     0,     0,    24,
      25,    26,    27,    28,    95,     0,     0,    30,     0,    31,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   165,    32,    33,    34,     0,
      35,     0,     0,     0,     0,     0,   158,     0,   159,    39,
      40,    41,    42,    43,    44,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    46,    47,     0,    48,    49,
      50,    51,    52,    53,     0,     0,     0,    54,    55,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    58,    59,    60,    61,    62,    63,     0,
      64,    65,    66,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
      74,    75,    76,     0,     0,     0,     0,     0,     0,     0,
     166,     0,   156,     0,     0,     0,     0,    77,    78,    79,
      80,    81,    82,    83,     0,     0,     2,    84,    85,    86,
       3,     4,    87,     0,    88,     0,    89,    90,    91,    92,
       7,     8,     0,    93,    94,    95,    96,    97,    98,    99,
     100,     0,     0,    13,     0,     0,     0,     0,     0,     0,
      14,    15,    16,    17,   157,     0,     0,     0,     0,     0,
       0,     0,    20,    21,    22,    23,     0,     0,    24,    25,
      26,    27,    28,     0,     0,     0,    30,     0,    31,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   123,     0,
       0,     0,     0,     0,     0,    32,    33,    34,     0,    35,
       0,     0,     0,     0,     0,   158,     0,   159,    39,    40,
      41,    42,    43,    44,     0,     0,     0,   124,     0,     0,
       0,     0,     0,     0,    46,    47,     0,    48,    49,    50,
      51,    52,    53,     0,     0,     0,    54,    55,     0,     0,
       0,     0,     0,     0,     0,     0,   125,     0,   126,   127,
     128,   129,    58,    59,    60,    61,    62,    63,     0,    64,
      65,    66,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   130,     0,     0,     0,     0,     0,     0,     0,    74,
      75,    76,   131,     0,     0,   132,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    77,    78,    79,    80,
      81,    82,    83,     0,     0,   133,    84,    85,    86,     0,
       0,    87,     0,    88,     0,    89,    90,    91,    92,     0,
       0,     0,    93,    94,    95,    96,    97,    98,    99,   100,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   134,   135,   136,     0,     0,     0,   137,   138
};

static const yytype_int16 yycheck[] =
{
      11,   101,    21,   110,    31,    79,    80,    69,   115,    19,
      20,    17,   197,    36,   179,    10,   218,   219,   203,   198,
      17,    34,    35,    92,    93,   248,   249,   250,    28,    24,
      92,    93,   197,    28,    29,   126,   197,    32,    33,    36,
     201,    94,   154,    38,    39,    40,    41,   270,    43,    97,
      98,    99,    62,   220,   198,    50,    51,   123,   197,   261,
     180,   181,   182,    58,    59,    60,    61,    62,    63,   197,
     144,   145,   197,   197,   197,    70,    71,    72,    73,   197,
     197,    76,    77,    78,    79,    80,   160,     0,    83,    84,
      26,    86,   197,   202,   198,    92,    93,   198,   198,   211,
     212,   213,   199,   198,   200,   217,   218,   197,   103,   104,
     105,   197,   107,   200,   199,   110,   198,   200,   113,   226,
     115,   116,   117,   118,   119,   120,   121,   122,   197,   198,
     197,   142,   143,   152,    27,   143,   178,   132,   133,    21,
     135,   136,   137,   138,   139,   140,   201,    26,     0,   144,
     145,    79,    80,     0,   172,   150,   143,   227,   232,    62,
     155,   184,   185,   186,   178,   160,   161,   162,   163,   164,
     165,   198,   167,   168,   169,   170,   171,   183,   173,   174,
     175,   176,   177,    25,    25,   229,   179,    26,   185,   186,
      96,   131,   187,   188,   189,   179,   198,   198,   201,    26,
      94,   111,   113,   202,   198,   143,   217,   218,   219,   204,
     205,   206,   207,   208,   209,   210,   144,   145,    -1,   214,
     215,   216,    -1,    -1,   219,    -1,   221,    -1,   223,   224,
     225,   226,   160,    10,    -1,   230,   231,   232,   233,   234,
     235,   236,   237,    -1,    -1,    -1,   246,    24,    -1,    -1,
     261,    28,    29,   264,   254,    -1,    -1,    -1,    -1,    -1,
      -1,    38,    39,    -1,    -1,    -1,    -1,    -1,    -1,   197,
      -1,    -1,    -1,    -1,    51,    -1,    -1,    -1,    -1,    -1,
      -1,    58,    59,    60,    61,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    70,    71,    72,    73,    -1,    -1,    76,
      77,    78,    79,    80,   232,    -1,    -1,    84,    -1,    86,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   102,   103,   104,   105,    -1,
     107,    -1,    -1,    -1,    -1,    -1,   113,    -1,   115,   116,
     117,   118,   119,   120,   121,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   132,   133,    -1,   135,   136,
     137,   138,   139,   140,    -1,    -1,    -1,   144,   145,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   160,   161,   162,   163,   164,   165,    -1,
     167,   168,   169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     187,   188,   189,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     197,    -1,    10,    -1,    -1,    -1,    -1,   204,   205,   206,
     207,   208,   209,   210,    -1,    -1,    24,   214,   215,   216,
      28,    29,   219,    -1,   221,    -1,   223,   224,   225,   226,
      38,    39,    -1,   230,   231,   232,   233,   234,   235,   236,
     237,    -1,    -1,    51,    -1,    -1,    -1,    -1,    -1,    -1,
      58,    59,    60,    61,    62,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    70,    71,    72,    73,    -1,    -1,    76,    77,
      78,    79,    80,    -1,    -1,    -1,    84,    -1,    86,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    37,    -1,
      -1,    -1,    -1,    -1,    -1,   103,   104,   105,    -1,   107,
      -1,    -1,    -1,    -1,    -1,   113,    -1,   115,   116,   117,
     118,   119,   120,   121,    -1,    -1,    -1,    66,    -1,    -1,
      -1,    -1,    -1,    -1,   132,   133,    -1,   135,   136,   137,
     138,   139,   140,    -1,    -1,    -1,   144,   145,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    95,    -1,    97,    98,
      99,   100,   160,   161,   162,   163,   164,   165,    -1,   167,
     168,   169,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   120,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   187,
     188,   189,   131,    -1,    -1,   134,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   204,   205,   206,   207,
     208,   209,   210,    -1,    -1,   154,   214,   215,   216,    -1,
      -1,   219,    -1,   221,    -1,   223,   224,   225,   226,    -1,
      -1,    -1,   230,   231,   232,   233,   234,   235,   236,   237,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   211,   212,   213,    -1,    -1,    -1,   217,   218
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int16 yystos[] =
{
       0,    10,    24,    28,    29,    32,    33,    38,    39,    40,
      41,    43,    50,    51,    58,    59,    60,    61,    62,    63,
      70,    71,    72,    73,    76,    77,    78,    79,    80,    83,
      84,    86,   103,   104,   105,   107,   110,   113,   115,   116,
     117,   118,   119,   120,   121,   122,   132,   133,   135,   136,
     137,   138,   139,   140,   144,   145,   150,   155,   160,   161,
     162,   163,   164,   165,   167,   168,   169,   170,   171,   173,
     174,   175,   176,   177,   187,   188,   189,   204,   205,   206,
     207,   208,   209,   210,   214,   215,   216,   219,   221,   223,
     224,   225,   226,   230,   231,   232,   233,   234,   235,   236,
     237,   239,   240,   250,   272,   273,   126,   198,    34,    35,
     268,   179,   197,    94,   245,   274,   220,   273,   123,    97,
      98,    99,   246,    37,    66,    95,    97,    98,    99,   100,
     120,   131,   134,   154,   211,   212,   213,   217,   218,    21,
     152,   198,   197,   201,   197,   197,   197,   197,   197,   197,
     197,     0,   197,    26,   198,   198,    10,    62,   113,   115,
     272,   180,   181,   182,   202,   102,   197,   272,   199,   198,
     200,   197,    92,    93,   197,   198,   197,   199,   198,   200,
     200,    69,    92,    93,    17,    36,    92,    93,   185,   186,
     197,    27,   244,   245,   244,   255,   257,   251,   253,    19,
      20,    62,    31,   198,   101,   198,   197,   203,   249,   201,
      26,   241,   242,   172,   143,   227,   178,   260,   258,   265,
     262,   267,   243,    62,    25,    25,   256,   229,   179,    17,
     183,   245,   247,   247,   247,    26,    36,   184,   185,   186,
     245,   248,    96,   131,   272,   179,   254,   198,   261,   259,
     266,   198,   201,    26,   252,   197,   270,   273,   248,   248,
     248,   263,    94,   273,   271,   247,   202,   111,   245,   269,
     264,   113,   248,   198
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int16 yyr1[] =
{
       0,   238,   239,   239,   240,   241,   240,   240,   242,   240,
     240,   240,   240,   240,   240,   240,   240,   240,   240,   240,
     240,   240,   240,   243,   240,   240,   240,   240,   240,   240,
     240,   240,   240,   240,   240,   240,   240,   240,   240,   240,
     240,   240,   240,   240,   240,   244,   244,   245,   246,   247,
     247,   247,   248,   248,   248,   248,   248,   249,   249,   250,
     251,   252,   250,   253,   254,   250,   250,   250,   250,   250,
     255,   256,   250,   250,   250,   257,   250,   250,   250,   250,
     250,   250,   250,   258,   259,   250,   260,   261,   250,   262,
     263,   264,   250,   265,   266,   250,   267,   250,   268,   250,
     250,   250,   250,   250,   250,   250,   250,   250,   269,   269,
     270,   271,   270,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   272,   272,   272,   272,   272,
     272,   272,   272,   272,   272,   273,   273,   273,   273,   273,
     273,   274,   274
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     1,     2,     0,     5,     3,     0,     5,
       3,     1,     1,     2,     4,     4,     3,     3,     3,     3,
       3,     3,     3,     0,     7,     3,     3,     3,     3,     3,
       3,     6,     9,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     2,     1,     2,     1,     1,     0,     1,     2,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       0,     0,     8,     0,     0,     7,     2,     2,     2,     2,
       0,     0,     6,     3,     2,     0,     4,     1,     3,     3,
       3,     1,     2,     0,     0,     8,     0,     0,     8,     0,
       0,     0,    11,     0,     0,     8,     0,     6,     0,     3,
       1,     1,     2,     2,     3,     3,     2,     4,     1,     3,
       1,     0,     3,     1,     1,     1,     2,     2,     2,     2,
       2,     2,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     2,     1,
       1,     1,     2
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YYUSE (yyoutput);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yytype], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyo, yytype, yyvaluep);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, int yyrule)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[+yyssp[yyi + 1 - yynrhs]],
                       &yyvsp[(yyi + 1) - (yynrhs)]
                                              );
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
#  else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                yy_state_t *yyssp, int yytoken)
{
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Actual size of YYARG. */
  int yycount = 0;
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[+*yyssp];
      YYPTRDIFF_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
      yysize = yysize0;
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYPTRDIFF_T yysize1
                    = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
                    yysize = yysize1;
                  else
                    return 2;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    /* Don't count the "%s"s in the final size, but reserve room for
       the terminator.  */
    YYPTRDIFF_T yysize1 = yysize + (yystrlen (yyformat) - 2 * yycount) + 1;
    if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
      yysize = yysize1;
    else
      return 2;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep)
{
  YYUSE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;


/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
    yy_state_fast_t yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss;
    yy_state_t *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYPTRDIFF_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    goto yyexhaustedlab;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
# undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex ();
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2:
                {
			// Accept if we're at/past the end of the cmd
			// or if only blanks and semicolons remain;
			// otherwise, syntax error.
			//
			unsigned pos = SqlciParse_InputPos-1;
			if (pos < strlen(SqlciParse_InputStr))
			  {
			    const char *s = &SqlciParse_InputStr[pos];
			    for ( ; *s; s++)
			      if (!isspace(*s) && *s != ';')
				{sqlcierror("");YYERROR;}
			  }



		   	SqlciParseTree = (SqlciNode *) (yyvsp[0].sqlci_cmd_type);
			YYACCEPT;
		}
    break;

  case 3:
                {
		   	SqlciParseTree = (SqlciNode *) (yyvsp[0].sql_cmd_type);
			YYACCEPT;
		}
    break;

  case 4:
                  {
			(yyval.sqlci_cmd_type) = new Mode (Mode::SQL_ , TRUE);
		  }
    break;

  case 5:
         {
		   identifier_name_internal = new char[strlen((yyvsp[-1].stringval_type))+1];
		   strcpy(identifier_name_internal, (yyvsp[-1].stringval_type));
		 }
    break;

  case 6:
         { 
			(yyval.sqlci_cmd_type) = new Log(identifier_name_internal, strlen(identifier_name_internal), Log::CLEAR_, (yyvsp[-2].intval_type));
		 }
    break;

  case 7:
                 { 
			(yyval.sqlci_cmd_type) = new Log((yyvsp[-1].stringval_type), strlen((yyvsp[-1].stringval_type)), Log::APPEND_, (yyvsp[0].intval_type));
		 }
    break;

  case 8:
         {
		   identifier_name_internal = new char[strlen((yyvsp[-1].stringval_type))+1];
		   strcpy(identifier_name_internal, (yyvsp[-1].stringval_type));
		 }
    break;

  case 9:
           {
	   (yyval.sqlci_cmd_type) = new Log(identifier_name_internal, strlen(identifier_name_internal), Log::CLEAR_, (yyvsp[-2].intval_type));
		 }
    break;

  case 10:
                 { 
			(yyval.sqlci_cmd_type) = new Log((yyvsp[-1].stringval_type), strlen((yyvsp[-1].stringval_type)), Log::APPEND_, (yyvsp[0].intval_type));
		 }
    break;

  case 11:
                  { 
			(yyval.sqlci_cmd_type) = new Log(0,0, Log::STOP_, 0);
		  }
    break;

  case 12:
                  { 
			(yyval.sqlci_cmd_type) = new Exit(0,0);
		  }
    break;

  case 13:
                  { 
		  (yyval.sqlci_cmd_type) = new Error((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), Error::DETAIL_);
		  }
    break;

  case 14:
                  { (yyval.sqlci_cmd_type) = new Error((yyvsp[-2].stringval_type), strlen((yyvsp[-2].stringval_type)), Error::BRIEF_);
		  }
    break;

  case 15:
                  { (yyval.sqlci_cmd_type) = new Error((yyvsp[-2].stringval_type), strlen((yyvsp[-2].stringval_type)), Error::DETAIL_);
		  }
    break;

  case 16:
                  { 
		  			(yyval.sqlci_cmd_type) = new SubError((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), SubError::HBASE_);
		  }
    break;

  case 17:
                  { 
		  			(yyval.sqlci_cmd_type) = new SubError((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), SubError::TM_);
		  }
    break;

  case 18:
                  {
		    int i, len, quoted;
                    CharInfo::CharSet cs;
                    int inhexadecimal;
                    EQuotedStringWithoutPrefixFlag quotedStrWithoutPrefixFlag;

		    if (trimValueText(i, len, quoted, cs, inhexadecimal, -1, 0, -1,
                                      &quotedStrWithoutPrefixFlag)) {sqlcierror("");YYERROR;}
		    //if (len <= 0 && !quoted) 	       {sqlcierror("");YYERROR;}

		    // see if input was of form "set param ?p null;".
		    // If yes, this param is a null value.
		    short null_param_val = 0;
#ifdef _EMPTYSTRING_EQUIVALENT_NULL
        if (len == 0)
          null_param_val = 1;
		    else if (len == 4 && !quoted)
#else
        if (len == 4 && !quoted)
#endif
		      {
			char buf[4];
			for (int k = 0; k < 4; k++)
			  buf[k] = toupper(SqlciParse_OriginalStr[i+k]);

			null_param_val = (strncmp(buf, "NULL", 4) == 0);
		      }

		    if (null_param_val)
		      (yyval.sqlci_cmd_type) = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), (char*)0, -1);
		    else {
                      char* pvalue = &SqlciParse_OriginalStr[i];

                      if ( inhexadecimal ) {

                         NAWString pvalue_in_wchar(CharInfo::ISO88591, pvalue);

                         void* result = NULL;
                         enum hex_conversion_code code = 
                           verifyAndConvertHex(pvalue_in_wchar, pvalue_in_wchar.length(), 
                                               L'\'', cs, &sqlci_Heap, result);

                         switch ( code ) {

                           case SINGLE_BYTE:
                           {
                             NAString* conv_pvalue = (NAString*)result;

		             (yyval.sqlci_cmd_type) = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), 
                                               (char*)(conv_pvalue->data()), conv_pvalue->length(), cs);
                             if (quotedStrWithoutPrefixFlag == eQUOTED_STRING_WITHOUT_CHARSET_PREFIX)
                             {
                               NAWString wstr(SqlciEnvGlobal->getTerminalCharset(),
                                              conv_pvalue->data(), conv_pvalue->length());
                               SetParam * pSetParam = (SetParam *)(yyval.sqlci_cmd_type);
                               pSetParam->setUTF16ParamStrLit(wstr.data(), wstr.length());
                               pSetParam->setQuotedStrWithoutPrefixFlag(TRUE);
                               pSetParam->setTermCharSet(SqlciEnvGlobal->getTerminalCharset());
                             }
// NADELETE() is not a const func while this routine is. Mask out the warning
                             NADELETE(conv_pvalue, NAString, &sqlci_Heap);
                           } 
                           break;

                           case DOUBLE_BYTE:
                           {
                             NAWString* conv_pvalue = (NAWString*)result;
		             (yyval.sqlci_cmd_type) = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), 
                                               (NAWchar*)(conv_pvalue->data()), conv_pvalue->length(), cs);
// NADELETE() is not a const func while this routine is. Mask out the warning
                             NADELETE(conv_pvalue, NAWString, &sqlci_Heap);
                           } 
                           break;

                           default:
                             sqlcierror("");YYERROR;
                         }
                      } else {

                        switch (cs) {
                           case CharInfo::UNICODE:
                            {
                               NAWString wstr(SqlciEnvGlobal->getTerminalCharset(), pvalue, len); 
		               (yyval.sqlci_cmd_type) = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), (NAWchar*)wstr.data(), wstr.length(), cs);
                            }
                            break;

                           case CharInfo::KANJI_MP:
                           case CharInfo::KSC5601_MP:

                            if ( len % 2 != 0 )
                              { 
                                // restore the closing two single quotes and ';' removed 
                                // inside the trimValueText() call. The string starts 
                                // at pvalue. We guarantee to have enough space here 
                                // because trimValueText() will bail out if no single 
                                // quotes present.

                                // We need to restore the closing single quotes and ';'
                                // so that in the second attempt of sqlci cmd parsing,
                                // the error can be caught again. Otherwise, _kanji'a' 
                                // becomes _kanjia after first parsing attempt. That
                                // string is legal and is different than the one actually
                                // typed. 

                                // It is difficult to remove the KLRUGE of 2nd parsing
                                // attempt in sqlciparser.cpp, func sqlci_parser().
                                // A lot of regressions will fail if we do so.
                                int k = strlen(pvalue);

                                for ( int x=k; x>=1; x-- ) {
                                  pvalue[x] = pvalue[x-1];
                                }
                                *pvalue = '\'';
                                *(pvalue+k+1) = '\'';
                                *(pvalue+k+2) = ';';
                                *(pvalue+k+3) = '\0';

                                sqlcierror("");YYERROR; 
                              }

		            (yyval.sqlci_cmd_type) = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), (NAWchar*)pvalue, len/2, cs);
                            break;

                            // case CharInfo::ISO88591:
                            // case CharInfo::UTF8:
                            // case CharInfo::SJIS:
                            // case CharInfo::GB2312:
                            // case CharInfo::GBK:
                            // case CharInfo::...:
                           default:
                            {
                              SetParam * pSetParam = NULL;
                              if (quotedStrWithoutPrefixFlag == eQUOTED_STRING_WITHOUT_CHARSET_PREFIX)
                              {
                                // SqlciEnvGlobal->getInferCharset() == TRUE || FALSE
                                pSetParam = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), pvalue, len, cs);
                                NAWString wstr(cs, pvalue, len);
                                pSetParam->setUTF16ParamStrLit(wstr.data(), wstr.length());
                                pSetParam->setQuotedStrWithoutPrefixFlag(TRUE);
                                pSetParam->setTermCharSet(SqlciEnvGlobal->getTerminalCharset());
                              }
                              else
                                pSetParam = new SetParam((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), pvalue, len, cs);
                              (yyval.sqlci_cmd_type) = pSetParam;
                            }
                            break;
                            
                        }
                      }
                    }
		  }
    break;

  case 19:
                  {
		    int i, len, quoted;
                    CharInfo::CharSet cs;
                    int inhexadecimal;
		    if (trimValueText(i, len, quoted, cs, inhexadecimal)) {sqlcierror("");YYERROR;}
		    (yyval.sqlci_cmd_type) = new SetPattern((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)),
					&SqlciParse_OriginalStr[i], len);
		  }
    break;

  case 20:
                  {
		    int i, len, quoted;
		    CharInfo::CharSet cs;
		    int inhexadecimal;
		    EQuotedStringWithoutPrefixFlag quotedStrWithoutPrefixFlag;
		    if ( trimValueText( i, len, quoted, cs, inhexadecimal
		                      , -1/*true*/ // quoting_allowed?
		                      , 0/*false*/ // use_i(1st_param)_as_is?
		                      , -1/*true*/ // cs_allowed?
		                      , &quotedStrWithoutPrefixFlag
		                      , -1/*true*/ // keepValueTextAsIs
		                      ) )
		    {sqlcierror("");YYERROR;}
		    (yyval.sqlci_cmd_type) = new SetPattern((yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)),
					&SqlciParse_OriginalStr[i], len);
		  }
    break;

  case 21:
                  {
		    (yyval.sqlci_cmd_type) = new Shape(TRUE, NULL, NULL);
		  }
    break;

  case 22:
                  {
		    (yyval.sqlci_cmd_type) = new Shape(FALSE, NULL, NULL);
		  }
    break;

  case 23:
                  {
		    identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
		    strcpy(identifier_name_internal, (yyvsp[0].stringval_type));
		  }
    break;

  case 24:
                  {
		    (yyval.sqlci_cmd_type) = new Shape(FALSE, identifier_name_internal, (yyvsp[0].stringval_type));
                  }
    break;

  case 25:
                  {
		    (yyval.sqlci_cmd_type) = new Statistics(0,0,Statistics::SET_OFF, NULL);
		  }
    break;

  case 26:
                  {
		    // display statistics in old format
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(char *)"of");
             	  }
    break;

  case 27:
                  {
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(char *)"PERTABLE");
		  }
    break;

  case 28:
                  {
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(char *)"PROGRESS");
		  }
    break;

  case 29:
                  {
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(char *)"DEFAULT");
		  }
    break;

  case 30:
                  {
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(char *)"ALL");
		  }
    break;

  case 31:
                  {
		    // display stats in new format
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON, NULL);
		  }
    break;

  case 32:
                  {
		    // display stats in new format based on options in
		    // quoted_string
		    (yyval.sqlci_cmd_type) = new Statistics(NULL, 0, Statistics::SET_ON,
					(yyvsp[0].stringval_type));
                  }
    break;

  case 33:
                {
		    (yyval.sqlci_cmd_type) = new SetTerminalCharset((yyvsp[0].stringval_type));
		}
    break;

  case 34:
                {
		    (yyval.sqlci_cmd_type) = new SetIsoMapping((yyvsp[0].stringval_type));
		}
    break;

  case 35:
                {
		    (yyval.sqlci_cmd_type) = new SetDefaultCharset((yyvsp[0].stringval_type));
		}
    break;

  case 36:
                {
		    (yyval.sqlci_cmd_type) = new SetInferCharset((yyvsp[0].stringval_type));
		}
    break;

  case 37:
                {
		    (yyval.sqlci_cmd_type) = new SetInferCharset((yyvsp[0].stringval_type));
		}
    break;

  case 38:
                {
		    (yyval.sqlci_cmd_type) = new SetInferCharset((char *)"TRUE");
		}
    break;

  case 39:
                {
		    (yyval.sqlci_cmd_type) = new SetInferCharset((char *)"FALSE");
		}
    break;

  case 40:
                  {
		    (yyval.sqlci_cmd_type) = new Reset(Reset::PARAM_, (yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)));
		  }
    break;

  case 41:
                  {
		    (yyval.sqlci_cmd_type) = new Reset(Reset::PATTERN_, (yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)));
		  }
    break;

  case 42:
                  {
		    if ((sqlcitext) && (sqlcitext[0] != ';'))
		      {sqlcierror("");YYERROR;}

		    (yyval.sqlci_cmd_type) = new Reset((yyvsp[0].reset_stmt_type_));
		  }
    break;

  case 43:
                  {
                    (yyval.sqlci_cmd_type) = new Wait(NULL, 0);
                  }
    break;

  case 44:
                  {
                    int v = atoi((yyvsp[0].stringval_type));
                    (yyval.sqlci_cmd_type) = new SleepVal(v);
                  }
    break;

  case 45:
                               { (yyval.intval_type) = 1; }
    break;

  case 46:
                     { (yyval.intval_type) = 0; }
    break;

  case 48:
                                {(yyval.reset_stmt_type_) = Reset::PARAM_;}
    break;

  case 49:
                                        { (yyval.intval_type) = atoi((yyvsp[0].stringval_type)); }
    break;

  case 50:
                                        { (yyval.intval_type) = 0; }
    break;

  case 51:
                                        { (yyval.intval_type) = 1; }
    break;

  case 52:
                                      { (yyval.intval_type) = SQLCLI_ACCUMULATED_STATS; }
    break;

  case 53:
                                      { (yyval.intval_type) = SQLCLI_PERTABLE_STATS; }
    break;

  case 54:
                                      { (yyval.intval_type) = SQLCLI_PROGRESS_STATS; }
    break;

  case 55:
                                      { (yyval.intval_type) = SQLCLI_SAME_STATS; }
    break;

  case 56:
                                      { (yyval.intval_type) = SQLCLI_DEFAULT_STATS; }
    break;

  case 57:
               { 
                // Convert IDENTIFIER to uppercase
                identifier_name_internal2 = new char[strlen((yyvsp[0].stringval_type))+1];
		str_cpy_convert(identifier_name_internal2, (yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), TRUE); 
                identifier_name_internal2[strlen((yyvsp[0].stringval_type))] = '\0';
                (yyval.stringval_type) = identifier_name_internal2;
              }
    break;

  case 58:
            { 
                // keep Double Quoted String as is                
                identifier_name_internal2 = new char[strlen((yyvsp[0].stringval_type))+1];
                strcpy(identifier_name_internal2, (yyvsp[0].stringval_type));
                (yyval.stringval_type) = identifier_name_internal2;
            }
    break;

  case 59:
                        {
                          (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, (yyvsp[0].dml_stmt_type), NULL);
                          REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 60:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			}
    break;

  case 61:
                        {
			  pos_internal = SqlciParse_InputPos;
			}
    break;

  case 62:
                        {
			  (yyval.sql_cmd_type) = 
	   		    new Cursor(identifier_name_internal, 
				       Cursor::DECLARE, -1,
				       &SqlciParse_OriginalStr[pos_internal]);
			  REPOSITION_SqlciParse_InputPos;

			  ((Cursor *)(yyval.sql_cmd_type))->setHoldable(TRUE);
			}
    break;

  case 63:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  (long) strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			}
    break;

  case 64:
                        {
			  pos_internal = (int) SqlciParse_InputPos;
			}
    break;

  case 65:
                        {
                          SqlciCursorInfo *cinfo = (yyvsp[0].cursor_info_type_);

			  Cursor *c = new
                            Cursor(identifier_name_internal, 
                                   Cursor::DECLARE,
                                   (short) cinfo->queryTextSpecified_,
                                   cinfo->queryTextOrStmtName_);

                          c->setResultSetIndex(cinfo->resultSetIndex_);

                          (yyval.sql_cmd_type) = c;

                          delete [] identifier_name_internal2;
                          identifier_name_internal2 = NULL;

                          delete cinfo;

			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 66:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  (yyval.sql_cmd_type) = 
			    new Cursor(identifier_name_internal, 
				       Cursor::OPEN, 0, NULL);
			}
    break;

  case 67:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  (yyval.sql_cmd_type) = 
			    new Cursor(identifier_name_internal, 
				       Cursor::FETCH, 0, NULL);
			}
    break;

  case 68:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  (yyval.sql_cmd_type) = 
			    new Cursor(identifier_name_internal, 
				       Cursor::CLOSE, 0, NULL);
			}
    break;

  case 69:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  (yyval.sql_cmd_type) = 
			    new Cursor(identifier_name_internal, 
				       Cursor::DEALLOC, 0, NULL);
			}
    break;

  case 70:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			}
    break;

  case 71:
                        {
			  pos_internal = SqlciParse_InputPos;
			}
    break;

  case 72:
                        {
			  (yyval.sql_cmd_type) = new Prepare(identifier_name_internal,
				     &SqlciParse_OriginalStr[pos_internal], (yyvsp[0].dml_stmt_type));
			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 73:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  (yyval.sql_cmd_type) = new DescribeStmt(identifier_name_internal, identifier_name_internal);
			}
    break;

  case 74:
                        {

			  if (sqlcitext[0] && sqlcitext[0] != ';')
			    {sqlcierror("");YYERROR;}

			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];

			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;

                          (yyval.sql_cmd_type) = new Execute(identifier_name_internal, 
                                           identifier_name_internal, 0/*flag*/, SqlciEnvGlobal);
			}
    break;

  case 75:
                        {
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];

			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
            }
    break;

  case 76:
            {
              pos_internal = SqlciParse_InputPos;
			  if (SqlciParse_OriginalStr[pos_internal-1] == ',')
			    pos_internal--;
                          (yyval.sql_cmd_type) = new Execute(identifier_name_internal, 
                                           &SqlciParse_OriginalStr[pos_internal], 1/*flag*/, SqlciEnvGlobal);

            }
    break;

  case 77:
                        {
                          (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DDL_TYPE, NULL);
                          REPOSITION_SqlciParse_InputPos;
                        }
    break;

  case 78:
                        {
			  // returns "EXPLAIN [options 'x'] id;", sets deprecated warning
			  // The +16 allows space to add "options 'm' " when defaulted
			  long newStrLen = strlen(SqlciParse_OriginalStr) + 16;
			  char * newStr = new char[newStrLen+1];
			  if ((yyvsp[-1].stringval_type))
			    {
			      strcpy(newStr, "EXPLAIN options '");
			      strcat(newStr, (yyvsp[-1].stringval_type));
			      strcat(newStr, "' ");
			    }
			  else
			    {
			      strcpy(newStr, "EXPLAIN options 'm' ");
			    }
			  identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];

			  str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
					  strlen((yyvsp[0].stringval_type)), TRUE);
			  identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;
			  strcat(newStr, identifier_name_internal);
			  strcat(newStr, ";");
			  sqlci_DA << DgSqlCode(15039);

			  (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);

			  delete identifier_name_internal;
			  delete newStr;
                        }
    break;

  case 79:
                        {
			  // Not a user-documented feature
			  // returns "EXPLAIN [options 'x'] procedure();", sets deprecated warning
			  // The +16 allows space to add "options 'm' " when defaulted
			  long newStrLen = strlen(SqlciParse_OriginalStr) + 16;
			  char * newStr = new char[newStrLen+1];
			  if ((yyvsp[-1].stringval_type))
			    {
			      strcpy(newStr, "EXPLAIN ");
			    }
			  else
			    {
			      strcpy(newStr, "EXPLAIN options 'm' ");
			    }
			  strcat(newStr, 
				 &SqlciParse_OriginalStr[strlen("DISPLAY_EXPLAIN")]);
			  sqlci_DA << DgSqlCode(15039);
			  (yyval.sql_cmd_type) = new DML(newStr,
				       DML_DESCRIBE_TYPE, NULL);
			  REPOSITION_SqlciParse_InputPos;
			  delete newStr;

                        }
    break;

  case 80:
                        {
			  // returns "EXPLAIN [options 'x'] SQL-cmd;", sets deprecated warning
			  // The +16 allows space to add "options 'm' " when defaulted
			  long newStrLen = strlen(SqlciParse_OriginalStr) + 16;
			  char * newStr = new char[newStrLen+1];
			  if ((yyvsp[-1].stringval_type))
			    {
			      strcpy(newStr, "EXPLAIN ");
			    }
			  else
			    {
			      strcpy(newStr, "EXPLAIN options 'm' ");
			    }
			  strcat(newStr, 
				 &SqlciParse_OriginalStr[strlen("DISPLAY_EXPLAIN")]);
			  sqlci_DA << DgSqlCode(15039);
			  (yyval.sql_cmd_type) = new DML(newStr,
				       DML_DESCRIBE_TYPE, NULL);
			  REPOSITION_SqlciParse_InputPos;
			  delete newStr;
			}
    break;

  case 81:
                  {
		    long newStrLen = 
		      strlen("GET STATISTICS , options 'of';");
		    char * newStr = new char[newStrLen+1];
		    strcpy(newStr, "GET STATISTICS , options 'of';");
		    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
		    
		    delete newStr;
		  }
    break;

  case 82:
                  {
		    identifier_name_internal = new char[strlen((yyvsp[0].stringval_type))+1];
		    
		    str_cpy_convert(identifier_name_internal, (yyvsp[0].stringval_type),
				    strlen((yyvsp[0].stringval_type)), TRUE);
		    identifier_name_internal[strlen((yyvsp[0].stringval_type))] = 0;

		    long newStrLen = 
		      strlen("GET STATISTICS FOR STATEMENT , options 'of';"); 
		    newStrLen += strlen(identifier_name_internal);
		    char * newStr = new char[newStrLen+1];
		    strcpy(newStr, "GET STATISTICS FOR STATEMENT ");
		    strcat(newStr, identifier_name_internal);
		    strcat(newStr, ", options 'of';");
		    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
		    
		    delete newStr;
		  }
    break;

  case 83:
                  {
                    pos_internal = atoi((yyvsp[0].stringval_type));
                  }
    break;

  case 84:
                  {
                    pos_internal3 = (yyvsp[0].intval_type);
                  }
    break;

  case 85:
                  {
                    char * newStr = new char[200];
                    sprintf(newStr, "GET STATISTICS FOR CPU %d ACTIVE %d", 
                            pos_internal, pos_internal3);
                    if ((yyvsp[0].intval_type) == SQLCLIDEV_ACCUMULATED_STATS)
                      strcat(newStr, " ACCUMULATED");
                    strcat(newStr, " ;");
                    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
                    delete newStr;
                  }
    break;

  case 86:
                  {
                    // This is of the form "FOR CPU \DOPEY.1"
                    identifier_name_internal2 = new char[strlen((yyvsp[0].stringval_type))+1];  
                    str_cpy_convert (identifier_name_internal2, (yyvsp[0].stringval_type), strlen ((yyvsp[0].stringval_type)), TRUE);
                    identifier_name_internal2[strlen((yyvsp[0].stringval_type))] = 0;
                  }
    break;

  case 87:
                  {
                    pos_internal3 = (yyvsp[0].intval_type);
                  }
    break;

  case 88:
                  {
                    
                    long strLen = 100+strlen(identifier_name_internal2);
                    char *newStr = new char[strLen];
                    sprintf( newStr, "GET STATISTICS FOR CPU %s ACTIVE %d",
                        identifier_name_internal2, pos_internal3);
                    if ((yyvsp[0].intval_type) == SQLCLIDEV_ACCUMULATED_STATS)
                        strcat(newStr, " ACCUMULATED");
                    strcat(newStr, " ;");
                    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
                    delete identifier_name_internal2;
                    delete newStr;
                  }
    break;

  case 89:
                  {
                    pos_internal = atoi ((yyvsp[0].stringval_type));
                  }
    break;

  case 90:
                  {
                    pos_internal2 = atoi ((yyvsp[0].stringval_type));
                  }
    break;

  case 91:
                  {
                    pos_internal3 = (yyvsp[0].intval_type);
                    
                  }
    break;

  case 92:
                  {
                    char *newStr = new char[200];
                    sprintf(newStr, "GET STATISTICS FOR PID %d,%d ACTIVE %d", 
                          pos_internal, pos_internal2, pos_internal3);
                    if ((yyvsp[0].intval_type) == SQLCLIDEV_ACCUMULATED_STATS)
                        strcat(newStr, " ACCUMULATED");
                    strcat(newStr, " ;");
                    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
                    delete newStr;
                  }
    break;

  case 93:
                  {
                    // This is of the form "FOR PID \DOPEY.1,5"
                    identifier_name_internal2 = new char[strlen((yyvsp[0].stringval_type))+1];  
                    str_cpy_convert (identifier_name_internal2, (yyvsp[0].stringval_type), strlen((yyvsp[0].stringval_type)), TRUE);
                    identifier_name_internal2[strlen((yyvsp[0].stringval_type))] = 0;
                  }
    break;

  case 94:
                  {
                    pos_internal3 = (yyvsp[0].intval_type);
                  }
    break;

  case 95:
                  {
                    long strLen = 100 + strlen(identifier_name_internal2);
                    char *newStr = new char[strLen];
                    sprintf(newStr, "GET STATISTICS FOR PID %s ACTIVE %d", 
                      identifier_name_internal2, pos_internal3);
                    if ((yyvsp[0].intval_type) == SQLCLIDEV_ACCUMULATED_STATS)
                        strcat(newStr, " ACCUMULATED");
                    strcat(newStr, " ;");
                    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
                    delete identifier_name_internal2;
                    delete newStr;
                  }
    break;

  case 96:
                  {
                    get_stats_str = new char[strlen((yyvsp[0].stringval_type))+100]; 
                    sprintf(get_stats_str, "GET STATISTICS FOR QID %s", (yyvsp[0].stringval_type));
                  }
    break;

  case 97:
                  {
                    char *newStr1 = new char[25];
                    if ((yyvsp[0].intval_type) == SQLCLI_ACCUMULATED_STATS)
                        strcpy(newStr1, " ACCUMULATED");
                    else if ((yyvsp[0].intval_type) == SQLCLI_PERTABLE_STATS)
                        strcpy(newStr1, " PERTABLE");
                    else if ((yyvsp[0].intval_type) == SQLCLI_PROGRESS_STATS)
                        strcpy(newStr1, " PROGRESS");
                    else if ((yyvsp[0].intval_type) == SQLCLI_SAME_STATS)
                        strcpy(newStr1, " DEFAULT");
                    else
                        strcpy(newStr1, "");
				    strcat(get_stats_str, newStr1);
					strcat(get_stats_str, " ;");
					(yyval.sql_cmd_type) = new DML(get_stats_str, DML_DESCRIBE_TYPE, NULL);
	                
					// delete id name internal2 allocated in qid_identifier
					if (identifier_name_internal2)
					{
					  delete [] identifier_name_internal2;
					  identifier_name_internal2 = NULL;
					}
					delete [] get_stats_str;
					delete [] newStr1;
				  }
    break;

  case 98:
                        {
			  pos_internal = 7;
			}
    break;

  case 99:
                        {
			  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, (yyvsp[0].dml_stmt_type),
				       "__SQLCI_DML_DISPLAY__");
			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 100:
                        {
			  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DISPLAY_NO_HEADING_TYPE, NULL);
			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 101:
                        {
			  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DESCRIBE_TYPE, NULL);
			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 102:
                        {
			  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DESCRIBE_TYPE, NULL);
			  REPOSITION_SqlciParse_InputPos;
			}
    break;

  case 103:
                {
		  long newStrLen = 
		    strlen("GET ENVVARS;");
		  char * newStr = new char[newStrLen+1];
		  strcpy(newStr, "GET ENVVARS;");
		  (yyval.sql_cmd_type) = new DML(newStr, DML_SHOWSHAPE_TYPE, NULL);
		  
		  delete newStr;
		}
    break;

  case 104:
                {
		  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DESCRIBE_TYPE, NULL);
		}
    break;

  case 105:
                {
		  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DESCRIBE_TYPE, NULL);
		}
    break;

  case 106:
                {
		  (yyval.sql_cmd_type) = new DML(SqlciParse_OriginalStr, DML_DESCRIBE_TYPE, NULL);
		}
    break;

  case 107:
                  { 
		    long newStrLen = 
		      strlen("GET TEXT FOR ERROR ") + 20;
		    char * newStr = new char[newStrLen+1];
		    str_sprintf(newStr, "GET TEXT FOR ERROR %s", (yyvsp[-2].stringval_type));
		    (yyval.sql_cmd_type) = new DML(newStr, DML_DESCRIBE_TYPE, NULL);
		  }
    break;

  case 108:
                    {
                      (yyval.intval_type) = 0;
                    }
    break;

  case 109:
                    {
                      (yyval.intval_type) = atoi((yyvsp[0].stringval_type));
                    }
    break;

  case 110:
              { 
                char *queryText = &SqlciParse_OriginalStr[pos_internal];
                char *queryTextCopy = new char[strlen(queryText) + 1];
                strcpy(queryTextCopy, queryText);

                SqlciCursorInfo *c = new SqlciCursorInfo();
                c->queryTextSpecified_ = -1;
                c->queryTextOrStmtName_ = queryTextCopy;
                (yyval.cursor_info_type_) = c;
              }
    break;

  case 111:
              {
                // Not sure exactly why, but we must make a copy of
                // the IDENTIFIER string now before more tokens are
                // parsed. If we wait until the entire rule is
                // matched, the IDENTIFIER string may not be valid
                // anymore. The copy will be pointed to by a global
                // variable and released later by the rule that
                // contains cursor_info as a subrule.
                identifier_name_internal2 = new char[strlen((yyvsp[0].stringval_type)) + 1];
                strcpy(identifier_name_internal2, (yyvsp[0].stringval_type));
              }
    break;

  case 112:
              {
                SqlciCursorInfo *c = new SqlciCursorInfo();
                c->queryTextSpecified_ = 0;
                c->queryTextOrStmtName_ = identifier_name_internal2;
                c->resultSetIndex_ = (yyvsp[0].intval_type);
                (yyval.cursor_info_type_) = c;
              }
    break;

  case 113:
                                        {}
    break;

  case 114:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 115:
                                        {(yyval.dml_stmt_type) = DML_OSIM_TYPE;}
    break;

  case 116:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 117:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 118:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 119:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 120:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 121:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 122:
                                        {(yyval.dml_stmt_type) = DML_UPDATE_TYPE;}
    break;

  case 123:
                                        {(yyval.dml_stmt_type) = DML_INSERT_TYPE;}
    break;

  case 124:
                                        {(yyval.dml_stmt_type) = DML_INSERT_TYPE;}
    break;

  case 125:
                                        {(yyval.dml_stmt_type) = DML_INSERT_TYPE;}
    break;

  case 126:
                                        {(yyval.dml_stmt_type) = DML_DELETE_TYPE;}
    break;

  case 127:
                                        {(yyval.dml_stmt_type) = DML_UPDATE_TYPE;}
    break;

  case 128:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 129:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 130:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 131:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 132:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 133:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 134:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 135:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 136:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 137:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 138:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 139:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 140:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 141:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 142:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 143:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 144:
                                     {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 145:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 146:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 147:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 148:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 149:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 150:
                                    {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 151:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 152:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 153:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 154:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 155:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 156:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 157:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 158:
                           {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 159:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 160:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 161:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 162:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 163:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 164:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 165:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 166:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 167:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 168:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 169:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 170:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 171:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 172:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 173:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 174:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 175:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 176:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 177:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 178:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 179:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 180:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 181:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 182:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 183:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 184:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 185:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 186:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 187:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 188:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 189:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 190:
                                        {(yyval.dml_stmt_type) = DML_DESCRIBE_TYPE;}
    break;

  case 191:
                                        {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 192:
                                   {(yyval.dml_stmt_type) = DML_DDL_TYPE;}
    break;

  case 193:
                                                {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 194:
                                        {(yyval.dml_stmt_type) = DML_CONTROL_TYPE;}
    break;

  case 195:
                                                {(yyval.dml_stmt_type) = DML_SELECT_TYPE;}
    break;

  case 196:
                                                {(yyval.dml_stmt_type) = DML_SELECT_TYPE;}
    break;

  case 197:
                                                {(yyval.dml_stmt_type) = DML_SELECT_TYPE;}
    break;

  case 198:
                                                {(yyval.dml_stmt_type) = DML_SELECT_TYPE;}
    break;

  case 199:
                                                {(yyval.dml_stmt_type) = DML_SELECT_TYPE;}
    break;

  case 200:
                                                {(yyval.dml_stmt_type) = DML_UNLOAD_TYPE;}
    break;

  case 201:
            { 
              (yyval.stringval_type) = 0; 
            }
    break;

  case 202:
            { 
              if (strcmp((yyvsp[0].stringval_type), "f") != 0)         // accept only old value
                {
                  sqlci_DA << DgSqlCode(-15517);// say invalid option
                  YYERROR;                      // give up processing
                }
              else
                {
                  (yyval.stringval_type) = (yyvsp[0].stringval_type);
                } 
            }
    break;



      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *, YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif


/*-----------------------------------------------------.
| yyreturn -- parsing is finished, return the result.  |
`-----------------------------------------------------*/
yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[+*yyssp], yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}

