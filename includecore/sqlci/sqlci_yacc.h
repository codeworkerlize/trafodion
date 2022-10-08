/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

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

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

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
