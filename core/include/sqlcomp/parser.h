#pragma once

#include "common/QueryText.h"
#include "parser/ulexer.h"
#include "sqlcomp/QCache.h"

class CmpContext;

class Parser {
 public:
  Parser(const CmpContext *cmpContext);
  virtual ~Parser();

  CmpContext *cmpContext();
  NAHeap *wHeap() { return wHeap_; }

  // locale-to-unicode conversion in parseDML requires buffer len (tcr)
  int parseDML(const char *str, int len, CharInfo::CharSet charset, ExprNode **node, int token = 0,
               ItemExprList *enl = NULL);
  // widens str to UNICODE for parsing, uses localized str for error handling

  // locale-to-unicode conversion in parseDML (for odbc/mx unicode support)
  int parseDML(QueryText &txt, ExprNode **node, int token = 0, ItemExprList *enl = NULL);
  // widens str to UNICODE for parsing, uses localized str for error handling

  int parse_w_DML(const NAWchar *str, int len, ExprNode **node, int token = 0, ItemExprList *enl = NULL);
  // narrows str to locale for error handling, uses wide str for parsing

  ExprNode *parseDML(const char *str, int len, CharInfo::CharSet charset);

  ExprNode *getExprTree(const char *str, UInt32 strlength = 0, CharInfo::CharSet strCharSet = CharInfo::UTF8,
                        int num_params = 0, ItemExpr *p1 = NULL, ItemExpr *p2 = NULL, ItemExpr *p3 = NULL,
                        ItemExpr *p4 = NULL, ItemExpr *p5 = NULL, ItemExpr *p6 = NULL,
                        ItemExprList *paramItemList = NULL,
                        int internal_expr = FALSE);  // pass this as TRUE if
                                                     // you know it is going
                                                     // to be an ItemExpr

  ItemExpr *getItemExprTree(const char *str, UInt32 strlength = 0, CharInfo::CharSet strCharSet = CharInfo::UTF8,
                            int num_params = 0, ItemExpr *p1 = NULL, ItemExpr *p2 = NULL, ItemExpr *p3 = NULL,
                            ItemExpr *p4 = NULL, ItemExpr *p5 = NULL, ItemExpr *p6 = NULL,
                            ItemExprList *paramItemList = NULL);

  ItemExpr *getItemExprTree(const char *str, UInt32 len, CharInfo::CharSet strCharSet, const ItemExprList &params);

  // wide versions of the above functions; used by catman to
  // process unicode-encoded column default value strings.
  ExprNode *get_w_ExprTree(const NAWchar *str, UInt32 strlength = 0, int num_params = 0, ItemExpr *p1 = NULL,
                           ItemExpr *p2 = NULL, ItemExpr *p3 = NULL, ItemExpr *p4 = NULL, ItemExpr *p5 = NULL,
                           ItemExpr *p6 = NULL, ItemExprList *paramItemList = NULL,
                           int internal_expr = FALSE);  // pass this as TRUE if
                                                        // you know it is going
                                                        // to be an ItemExpr

  ItemExpr *get_w_ItemExprTree(const NAWchar *str, UInt32 strlength = 0, int num_params = 0, ItemExpr *p1 = NULL,
                               ItemExpr *p2 = NULL, ItemExpr *p3 = NULL, ItemExpr *p4 = NULL, ItemExpr *p5 = NULL,
                               ItemExpr *p6 = NULL, ItemExprList *paramItemList = NULL);

  // parse the column definition, called from internal stored procedure
  // component ( i.e. CmpStoredProc.C )

  ElemDDLColDef *parseColumnDefinition(const char *str, size_t strLen, CharInfo::CharSet strCharSet);
  ElemDDLPartitionClause *parseSplitDefinition(const char *str, size_t strLen, CharInfo::CharSet strCharSet);

  NAType *parseCompositeDefinition(const char *str, size_t strLen, CharInfo::CharSet strCharSet);

  // part of interface to Unicode lexer
  yyULexer *getLexer() { return lexer; }
  int yylex(YYSTYPE *lvalp) {
    int retCode = lexer ? lexer->yylex(lvalp) : 0;
    addTokenToNormalizedString(retCode);
    return retCode;
  }
  const NAWchar *YYText() { return lexer ? lexer->YYText() : WIDE_(""); }
  int YYLeng() { return lexer ? lexer->YYLeng() : 0; }

  char *inputStr() { return inputBuf_ ? (char *)(inputBuf_->data()) : NULL; }
  charBuf *getInputcharBuf() { return inputBuf_; }
  size_t inputStrLen();  // size (in bytes) of inputBuf_ with trailing null characters excluded from the count
  CharInfo::CharSet inputStrCharSet() { return inputStr() == NULL ? CharInfo::UnknownCharSet : charset_; }
  NAWchar *wInputStr() { return wInputBuf_ ? wInputBuf_->data() : NULL; }
  NAWcharBuf *getInputNAWcharBuf() { return wInputBuf_; }
  size_t wInputStrLen();  // size (in NAWchars) of wInputBuf_ with trailing null characters excluded from the count

  NABoolean fixupParserInputBufAndAppendSemicolon();   // returns TRUE if new inputBuf_ is (re)allocated
  NABoolean fixupParserWInputBufAndAppendSemicolon();  // returns TRUE if new wInputBuf_ is (re)allocated

  NABoolean CharHereIsDoubleQuote(StringPos p) { return wInputStrLen() > p && wInputStr()[p] == NAWCHR('"'); }

  // This replaces the global variable SqlParser_InputStr in arkcmp.
  // SqlParser_InputStr is bad news to recursive parser calls. For
  // example, when arkcmp executes "create table t(a char not null)", it
  // calls CatalogManager::executeDDL() which calls CatCommand() which
  // parses the above statment and then calls CatCommand::execute()
  // which eventually calls CatAddNotNullConstraint() which calls the
  // parser again to process "alter table t add constraint blah check
  // (a is not null)". The partially unicode-enabled parser does the
  // unicode conversion of the input string very late: just before parsing.
  // This conversion may require memory allocation and deallocation. The
  // end result can be a ComASSERT() failure and possibly an arkcmp crash.
  // (tcr)

  void reset(NABoolean on_entry_reset_was_needed = FALSE);

  // set to one of: NORMAL_TOKEN=0, INTERNALEXPR_TOKEN=1, COLUMNDEF_TOKEN=2,
  //                SPLITDEF_TOKEN=3, COMPOSITEDEF_TOKEN=4;
  // used by the catalog manager for scanning/parsing odd stuff like:
  // "CAST('<minvalue>' AS CHAR(n))" (tcr)
  int internalExpr_;

  // the original client locale's character set; used by ulexer to convert
  // unicode string literals back to their original multibyte char form.
  CharInfo::CharSet charset_;
  CharInfo::CharSet initialInputCharSet_;

  // if this is not set to UnknownCharSet, then it is used during col create if one
  // is not explicitly specified.
  CharInfo::CharSet defaultColCharset_;

  CharInfo::CharSet defaultColCharset() { return defaultColCharset_; }

  void setmodeSpecial1(NABoolean v) { modeSpecial1_ = v; }
  NABoolean modeSpecial1() { return modeSpecial1_; }
  void setmodeSpecial4(NABoolean v) { modeSpecial4_ = v; }
  NABoolean modeSpecial4() { return modeSpecial4_; }

  void pushHasOlapFunctions(NABoolean v) { hasOlapFunctions_.insert(v); }
  NABoolean topHasOlapFunctions() { return hasOlapFunctions_[hasOlapFunctions_.entries() - 1]; }
  void setTopHasOlapFunctions(NABoolean v) { hasOlapFunctions_[hasOlapFunctions_.entries() - 1] = v; }
  NABoolean popHasOlapFunctions() { return hasOlapFunctions_.removeAt(hasOlapFunctions_.entries() - 1); }
  void clearHasOlapFunctions() { hasOlapFunctions_.clear(); }
  int hasOlapFunctionsEntries() { return hasOlapFunctions_.entries(); }
  void pushHasTDFunctions(NABoolean v) { hasTDFunctions_.insert(v); }
  NABoolean topHasTDFunctions() { return hasTDFunctions_[hasTDFunctions_.entries() - 1]; }
  void setTopHasTDFunctions(NABoolean v) { hasTDFunctions_[hasTDFunctions_.entries() - 1] = v; }
  void setTopHasConnectBy(NABoolean v) { hasConnectBy_ = v; }
  NABoolean topHasConnectBy() { return hasConnectBy_; }
  NABoolean popHasTDFunctions() { return hasTDFunctions_.removeAt(hasTDFunctions_.entries() - 1); }
  void clearHasTDFunctions() { hasTDFunctions_.clear(); }
  int hasTDFunctionsEntries() { return hasTDFunctions_.entries(); }

  NAList<ItemExpr *> &topPotentialOlapFunctions() { return *(potentialOlapFunctions_[hasOlapFunctionsEntries() - 1]); }
  void pushPotentialOlapFunctions() { potentialOlapFunctions_.insert(new (wHeap()) NAList<ItemExpr *>(wHeap())); }
  void popPotentialOlapFunctions() { potentialOlapFunctions_.removeAt(hasOlapFunctions_.entries() - 1); }

  HQCParseKey *getHQCKey() { return HQCKey_; }

  void setHQCKey(HQCParseKey *k) { HQCKey_ = k; }

  void addTokenToNormalizedString(int &tokCod) {
    if (HQCKey_) HQCKey_->addTokenToNormalizedString(tokCod);
  }

  void FixupForUnaryNegate(BiArith *itm) {
    if (HQCKey_) HQCKey_->FixupForUnaryNegate(itm);
  }

  void collectItem4HQC(ItemExpr *itm) {
    if (HQCKey_) HQCKey_->collectItem4HQC(itm);
  }

  void setIsHQCCacheable(NABoolean b) {
    if (HQCKey_) HQCKey_->setIsCacheable(b);
  }

  NABoolean isHQCCacheable() { return HQCKey_ ? HQCKey_->isCacheable() : FALSE; }

  NABoolean hasWithDefinition(NAString *key) {
    if (with_clauses_->contains(key))
      return TRUE;
    else
      return FALSE;
  }

  void insertWithDefinition(NAString *key, RelExpr *val) { with_clauses_->insert(key, val); }

  RelExpr *getWithDefinition(NAString *key) { return with_clauses_->getFirstValue(key); }

  Hint *getHint() const { return pHint_; }

  void setHint(Hint *h) { pHint_ = h; }
  NAHashDictionary<NAString, NAString> *getOrNewCqdHashDictionaryInHint();
  int SetAndHoldCqdsInHint();
  NABoolean hasCqdsInHint() { return hasCqdsInHint_; }
  void setCqdsInHint(NABoolean b) { hasCqdsInHint_ = b; }

 private:
  HQCParseKey *HQCKey_;

  // See notes in .C file.
  CmpContext *cmpContext_;
  Parser *prevParser_;

  NAHeap *wHeap_;              // Pointer to the NAHeap
  NABoolean hasInternalHeap_;  // Did Parser allocate this heap?

  // private methods for internal usage.

  // parseUtilISPCommand parse the input query for utility keyword and
  // generate a StmtQuery ( RelRoot ( RelInternalSP ) ) tree. The tree
  // is generated in this routine to bypass the arkcmp parser, because
  // the utility stored procedure will parse the parameter. Since there
  // might be quoted strings in the parameters, arkcmp parser can't parse
  // the parameters, it might destroy the parameters. This routine returns
  // TRUE in the case of utility keyword found and tree generated.
  // FALSE otherwise.
  NABoolean parseUtilISPCommand(const char *commandText, size_t commandTextLen, CharInfo::CharSet commandCharSet,
                                ExprNode **node);

  // parse input query for a Rel1 NSK DDL, UPDATE STATISTICS or special
  // CAT API requests. Create DDL Expr here
  // instead of letting it go thru the MX parser.
  NABoolean processSpecialDDL(const char *commandText, size_t commandTextLen, ExprNode *childNode,
                              CharInfo::CharSet commandTextCharSet, ExprNode **node);

  int parseSQL(ExprNode **node, int token = 0, ItemExprList *enl = NULL);

  void ResetLexer(void);
  yyULexer *lexer;

  charBuf *inputBuf_;
  NAWcharBuf *wInputBuf_;

  NABoolean modeSpecial1_;
  NABoolean modeSpecial4_;

  LIST(NABoolean) hasOlapFunctions_;
  LIST(NABoolean) hasTDFunctions_;
  LIST(NAList<ItemExpr *> *) potentialOlapFunctions_;
  /*
   * hashmap to save WITH clause definition
   * key is the name of the with clause
   * value is the RelExpr structure
   */
  NAHashDictionary<NAString, RelExpr> *with_clauses_;

  NABoolean hasConnectBy_;
  Hint *pHint_;  // uesd by CQDs in Hint
  NAHashDictionary<NAString, NAString> *pCqdsHashDictionaryInHint_;
  NABoolean hasCqdsInHint_;
};

#define PARSERASSERT(b)                                   \
  if (!(b)) {                                             \
    ParserAssertInternal(" " #b " ", __FILE__, __LINE__); \
  }
#define PARSERABORT(b)                                   \
  if (!(b)) {                                            \
    ParserAbortInternal(" " #b " ", __FILE__, __LINE__); \
  }

void ParserAssertInternal(const char *, const char *, int);
void ParserAbortInternal(const char *, const char *, int);

// The parsing routine which the preprocessor must call.
int sql_parse(const char *str, int len, CharInfo::CharSet charset, StmtNode **stmt_node_ptr_ptr);

charBuf *parserUTF16ToCharSet(const NAWcharBuf &pr_UTF16StrBuf, CollHeap *pp_Heap, charBuf *&pr_pOutCharSetStrBuf,
                              int pv_iCharSet, int &pr_iErrorcode, NABoolean pv_bAddNullAtEnd = TRUE,
                              NABoolean pv_bAllowInvalidCodePoint = TRUE, int *pp_iCharCount = NULL,
                              int *pp_iErrorByteOff = NULL);
