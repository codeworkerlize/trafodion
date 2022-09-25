// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#pragma once
//
//  Classifier.h
//
//  This file defines the Classifier object. The Classifier
//  attempts to determine characteristics about identifier and
//  constant tokens depending on surrounding tokens. It also
//  enters these into the Dictionary.
//

#include <list>

// Forward references
class Scanner;
class Dictionary;
class Token;

class Classifier
{
public:
    Classifier(Scanner & scanner, Dictionary & dictionary);
    ~Classifier(void);

    list<const Token *> & classify(void);

private:

    Scanner & scanner_;
    Dictionary & dictionary_;

    enum ParseState {
        NOTHING_SPECIAL,
        SAW_FROM,
        SAW_FROM_IDENT1,
        SAW_FROM_DOT1,
        SAW_FROM_IDENT2,
        SAW_FROM_DOT2,
        SAW_FROM_IDENT3,
        SAW_TABLE,
        SAW_TABLE_IDENT1,
        SAW_TABLE_DOT1,
        SAW_TABLE_IDENT2,
        SAW_TABLE_DOT2,
        SAW_CONTROL,
        SAW_CONTROL_QUERY,
        SAW_CONTROL_DEFAULT,
        SAW_CONTROL_IDENT,
        SAW_SIZED_DATATYPE,
        SAW_SD_LPAREN,
        SAW_SD_SIZE,
        SAW_SCHEMA,
        SAW_SCHEMA_IDENT,
        SAW_SCHEMA_DOT,
        SAW_ORDER_OR_GROUP,
        SAW_ORDER_BY_OR_GROUP_BY,
        SAW_NONOBFUSCATED_COMMAND
        // TODO: consider a correlation name after a right paranthesis too (e.g., nested select)
    };

    bool commandState_;  // true if next non-whitespace token is the beginning of a SQL statement

    ParseState parseState_;
    const Token * ident1_;
    const Token * ident2_;
    const Token * ident3_;

    list <const Token *> classifiedTokenList_;
};
