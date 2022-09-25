// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#pragma once
//
//  Token.h
//
//  This file defines the Token object.
//

#include <string>

using namespace std;

class Token
  {
  public:
    
    enum TokenType {
        TOKEN_SCALAR_OPERATOR,
        TOKEN_PARENTHESIS,
        TOKEN_STRING_CONSTANT,
        TOKEN_IDENTIFIER,
        TOKEN_FROM,
        TOKEN_TABLE,
        TOKEN_SCHEMA,
        TOKEN_OTHER_SQL_KEYWORD,
        TOKEN_NUMERIC_CONSTANT,
        TOKEN_DOT,
        TOKEN_COMMA,
        TOKEN_SEMICOLON,
        TOKEN_WHITESPACE,
        TOKEN_CONTROL,
        TOKEN_QUERY,
        TOKEN_DEFAULT,
        TOKEN_CQD,
        TOKEN_CQDNAME,
        TOKEN_TIME,
        TOKEN_TIMESTAMP,
        TOKEN_SIZED_DATATYPE,
        TOKEN_DIRECTIVE,
        TOKEN_NONOBFUSCATED_COMMAND_VERB,
        TOKEN_ORDER_OR_GROUP,
        TOKEN_BY,
        TOKEN_OTHER
      };
    
    Token(TokenType t, const char * start, const char * end);   
    ~Token();

    TokenType type(void) const
    {
        return type_; 
    };

    void setType(TokenType t);

    const string & originalText(void) const;
    const string & normalizedText(void) const;

    void display(void) const;

  private:

    void normalizeIdentifier(void);
    TokenType keywordOrIdentifier(void);

    TokenType type_;
    string originalText_;
    string normalizedText_;


  };
