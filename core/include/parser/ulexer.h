

#ifndef __U_LEXER_H
#define __U_LEXER_H

#include <ctype.h>
#include <stdio.h>
#include "common/charinfo.h"

// Forward references.
class ParKeyWord;

// UR2-CNTNSK
#define TXT(s) WIDE_(s)  // macro for Unicode string literals

extern "C++" {

struct yy_buffer_state;
typedef int yy_state_type;

union YYSTYPE;

class ULexer {
 public:
  virtual ~ULexer() {}

  const NAWchar *YYText() { return yytext_; }
  int YYLeng() { return yyleng_; }

  virtual int yylex(YYSTYPE *lvalp) = 0;

  int debug() const { return yy_U_debug_; }
  void set_debug(int flag) { yy_U_debug_ = flag; }

 protected:
  NAWchar *yytext_;
  int yyleng_;
  int yy_U_debug_;  // only has effect with -d or "%option debug"

  void yyToUpper()

  {
    for (NAWchar *c = yytext_; *c; c++) *c = toupper(*c);
  }

  char yynarrow_[400];
  void yyToNarrow() {
    char *n = yynarrow_;
    char *eob = n + sizeof(yynarrow_) - 1;
    for (NAWchar *c = yytext_; *c; c++, n++) {
      assert(n < eob);
      *n = (char)*c;
      NAWchar w = *n;
      assert(w == *c);
    }
    *n = '\0';
  }
};
}
#endif  // __U_LEXER_H

#if defined(yyULexer) || !defined(yyULexerOnce)
// Either this is the first time through (yyULexerOnce not defined),
// or this is a repeated include to define a different flavor of
// yyULexer, as discussed in the flex man page.
#define yyULexerOnce

class yyULexer : public ULexer {
 public:
  // construct lexer to scan an in-memory string
  yyULexer(const NAWchar *str, int charCount);
  yyULexer(const NAWchar *str, size_t charCount);

  virtual ~yyULexer();

  virtual int yylex(YYSTYPE *lvalp);

  void reset();

  // these 2 replace the old SqlParser_InputPos global variable
  int getInputPos();
  void setInputPos(int i);

  void setReturnAllChars() { returnAllChars_ = TRUE; }
  void resetReturnAllChars() { returnAllChars_ = FALSE; }

  NABoolean isDynamicParameter(int tokCod);

  NABoolean isLiteral4HQC(int tokCod);

 protected:
  void yyULexer_ctor(const NAWchar *str, int charCount);
  int input_pos_;  // used only by {set|get}InputPos()

  void yy_load_buffer_state();

  struct yy_buffer_state *yy_current_buffer_;

  // yy_hold_char_ holds the character lost when yytext_ is formed.
  NAWchar yy_hold_char_;

  // Number of characters read into yy_ch_buf.
  int yy_n_chars_;

  // Points to current character in buffer.
  NAWchar *yy_c_buf_p_;

  int yy_init_;  // whether we need to initialize

  NAWchar *beginRun_;  // points to start of a run
  NAWchar *currChar_;  // points to current candidate end of run

  NABoolean returnAllChars_;

  // set up yytext_, etc for the start of a scan
  void startRun() {
    currChar_ = yy_c_buf_p_;
    *currChar_ = yy_hold_char_;
    yytext_ = beginRun_ = currChar_;
  }

  // Done after the current pattern has been matched and before the
  // corresponding action - sets up yytext_.
  void doBeforeAction() {
    yytext_ = beginRun_;
    yyleng_ = (int)(currChar_ - beginRun_);
    input_pos_ = 0;
    yy_hold_char_ = *currChar_;
    *currChar_ = '\0';
    yy_c_buf_p_ = currChar_;
  }

  // un-null terminate yytext_. used in scanning compound tokens.
  void undoBeforeAction() { *yy_c_buf_p_ = yy_hold_char_; }

  // useful after an advance()
  int YYLengNow() { return (int)(currChar_ - beginRun_); }

  // used to remember candidate end of a compound token.
  NAWchar *mark() { return currChar_; }

  // used to retract current char pointer in compound token scanning
  void retractToMark(NAWchar *m) { currChar_ = m; }

  // have we reached the end of buffer?
  int endOfBuffer();

  // advance current character
  void advance() { currChar_++; }

  // delete comparison ' ' And '\n'
  void advanceDeleteSpaceAndEnter() {
    if ((*currChar_ == '>') || (*currChar_ == '<') || (*currChar_ == '!')) {
      currChar_++;
      while (*currChar_ == ' ' || *currChar_ == '\n') {
        currChar_++;
      }
    } else {
      currChar_++;
    }
  }

  // read current character; if end of buffer then refill it first.
  // returns WEOF or current character.
  NAWchar peekChar();

  // return current character and then advance
  NAWchar peekAdvance() {
    NAWchar c = peekChar();
    advance();
    return c;
  }

  // set current character to c
  void setCurrChar(NAWchar c) { *currChar_ = c; }

  // does lexer actions associated with recognition of one of:
  // {Reserved IDENTIFIER, IDENTIFIER, SQL/MX keyword, compound
  // keyword, compound Cobol token, approx numeric, exact numeric
  // with scale, exact numeric no scale}
  int anSQLMXReservedWord(YYSTYPE *lvalp);
  int anIdentifier(YYSTYPE *lvalp);
  int anSQLMXKeyword(int tokCod, YYSTYPE *lvalp);
  int aCompoundKeyword(int tokCod, YYSTYPE *lvalp);
  int aCobolToken(int tokCod, YYSTYPE *lvalp);
  int anApproxNumber(YYSTYPE *lvalp);
  int exactWithScale(YYSTYPE *lvalp);
  int exactNoScale(YYSTYPE *lvalp);
  int eitherCompoundOrSimpleKeyword(NABoolean isCompound, int tokcodCompound, int tokcodSimple, NAWchar *end1,
                                    NAWchar holdChar1, YYSTYPE *lvalp);
  int notCompoundKeyword(const ParKeyWord *key, NAWchar &holdChar, YYSTYPE *lvalp);

  int aCompoundStmtBlock(int tokCode, YYSTYPE *lvalp);
  int aQuotedBlock(YYSTYPE *lvalp);

  int aStringLiteralWithCharSet(CharInfo::CharSet, const NAWchar *s, int len, NAWchar quote, YYSTYPE *lvalp);

  // qualified hexadecimal format string literals
  int aHexStringLiteralWithCharSet(CharInfo::CharSet, const NAWchar *s, int len, NAWchar quote, YYSTYPE *lvalp);
  int constructStringLiteralWithCharSet(NABoolean hexFormat, CharInfo::CharSet cs, YYSTYPE *lvalp,
                                        NAWchar quote = L'\'');

  // helper functions to set yylval token value used by above functions
  int setStringval(int tokCod, const char *dbgstr, YYSTYPE *lvalp);
  int setTokval(int tokCod, const char *dbgstr, YYSTYPE *lvalp);

  int prematureEOF(YYSTYPE *lvalp);      // hit EOF inside a string or comment
  int invalidHexStrLit(YYSTYPE *lvalp);  // invalid format of hexadecimal representation of a string literal
  int invalidStrLitNonTranslatableChars(YYSTYPE *lvalp);   // invalid string literal/host var name
  int invalidHostVarNonTranslatableChars(YYSTYPE *lvalp);  // due to non-translatable characters.

  void addTokenToGlobalQueue(NABoolean isComment = FALSE);

};  // class yyULexer

#endif  // defined(yyULexer) || ! defined(yyULexerOnce)
