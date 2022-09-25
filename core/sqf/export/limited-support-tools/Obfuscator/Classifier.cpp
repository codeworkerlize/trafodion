// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Classifier.cpp
//
//  This file implements the Classifier object. The Classifier
//  attempts to determine characteristics about identifier and
//  constant tokens depending on surrounding tokens. It also
//  enters these into the Dictionary.
//

#include <list>
#include <iostream>  // for debugging

using namespace std;

#include "Classifier.h"
#include "Scanner.h"
#include "Dictionary.h"
#include "Token.h"

Classifier::Classifier(Scanner & scanner, Dictionary & dictionary)
    : scanner_(scanner), dictionary_(dictionary), 
    commandState_(true), parseState_(Classifier::NOTHING_SPECIAL),
    ident1_(0),ident2_(0),ident3_(0)
{
    // nothing more to do
}

Classifier::~Classifier(void)
{
    // nothing more to do

    // TODO: Perhaps this dtor should destroy the token list?
}

list<const Token *> & Classifier::classify(void)
{
    for (Token * token = scanner_.getNextToken();
    token;
        token = scanner_.getNextToken())
    {
        // TODO: The FROM clause could have a comma-separated list of object
        // names (the old-style join syntax)

        // TODO: Handle schema names (e.g., CREATE/DROP SCHEMA)
        // Partly done; need to allow for two-part names.

        // TODO: Handle $$xxx$$ identifiers (sqlci substitution variables). Treat
        // like a regular identifier. Normalized text should be $$XXX$$.

        // TODO: Consider not obfuscating statement names (e.g., PREPARE, EXPLAIN, EXECUTE)

        // TODO: Look for contexts where we use numbers to refer to positions
        // in a select list (e.g., TRANSPOSE) and convert such tokens
        // to TOKEN_OTHER. Another example: after GENERATE in update stats command.
        // Another example: after FOR in SUBSTRING.
        // Datatype sizes, e.g., CHAR(10) have been taken care of. ORDER BY and GROUP BY
        // have also been taken care of.

        // TODO: Look for other contexts where we should not obfuscate strings.
        // Have already taken care of CQD values.

        // TODO: If there is type information about a string constant (e.g.,
        // TIMESTAMP '1900-01-01 00:00:00') pass that info along as context

        // TODO: When entering NOTHING_SPECIAL state, seems like it would be
        // good to reprocess the current token. That will reduce the 
        // complexity where two syntactic structures can abut one another.

        // The most pressing work seems to be the last one.


        //cout << "Token returned by scanner: ";  debugging code
        //token->display();

        if (token->type() != Token::TOKEN_WHITESPACE)  // white space is not syntactically significant
        {
            // maintain parse state and take semantic actions

            switch (parseState_)
            {
            case NOTHING_SPECIAL:
            {
                if (token->type() == Token::TOKEN_FROM)
                    parseState_ = SAW_FROM;
                else if (token->type() == Token::TOKEN_TABLE)
                    parseState_ = SAW_TABLE;
                else if (token->type() == Token::TOKEN_SCHEMA)
                    parseState_ = SAW_SCHEMA;
                else if (token->type() == Token::TOKEN_CONTROL)
                    parseState_ = SAW_CONTROL;
                else if (token->type() == Token::TOKEN_CQD)
                    parseState_ = SAW_CONTROL_DEFAULT;
                else if (token->type() == Token::TOKEN_ORDER_OR_GROUP)
                    parseState_ = SAW_ORDER_OR_GROUP;
                else if ((token->type() == Token::TOKEN_TIME) ||
                    (token->type() == Token::TOKEN_TIMESTAMP) ||
                    (token->type() == Token::TOKEN_SIZED_DATATYPE))
                    parseState_ = SAW_SIZED_DATATYPE;
                else if ((token->type() == Token::TOKEN_NONOBFUSCATED_COMMAND_VERB) && commandState_)
                    parseState_ = SAW_NONOBFUSCATED_COMMAND;  // sqlci OBEY or LOG or SH command
                else if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    // could be any kind of name; we'll depend on seeing the same symbol
                    // in other contexts to distinguish it
                    dictionary_.addSymbol(token->normalizedText(), 0);
                    // remain in NOTHING_SPECIAL state
                }
                break;
            }  
            case SAW_FROM:  // TODO: can FROM occur anywhere else besides the FROM clause? (e.g., substring fn?)
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident1_ = token;
                    parseState_ = SAW_FROM_IDENT1;
                }
                else
                {
                    parseState_ = NOTHING_SPECIAL;
                    // TODO: Could be nested select, and would like to find correlation name
                    // and any other object names after it (e.g, a nested select followed by a comma and another table expr)
                }
                break;
            }
            case SAW_FROM_IDENT1:
            {
                if (token->type() == Token::TOKEN_DOT)
                    parseState_ = SAW_FROM_DOT1;
                else
                {
                    // must be ident1_ is a table name context (could be view or correlation name)
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident1_ = 0;
                    if (token->type() == Token::TOKEN_IDENTIFIER)
                        // there's another identifier after it; must be a correlation name
                        // (note that we treat AS as whitespace, so we don't have to check for it)
                        dictionary_.addSymbol(token->normalizedText(), Dictionary::CORRELATION_NAME);

                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_FROM_DOT1:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident2_ = token;
                    parseState_ = SAW_FROM_IDENT2;
                }
                else
                {
                    // this is actually a SQL syntax error; but let's assume ident1_ is an object name
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident1_ = 0;
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_FROM_IDENT2:
            {
                if (token->type() == Token::TOKEN_DOT)
                    parseState_ = SAW_FROM_DOT2;
                else
                {
                    // must be ident1_ is a schema name
                    // must be ident2_ is a table name context (could be view)
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::SCHEMA_NAME);
                    ident1_ = 0;
                    dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident2_ = 0;
                    if (token->type() == Token::TOKEN_IDENTIFIER)
                        // there's another identifier after it; must be a correlation name
                        // (note that we treat AS as whitespace, so we don't have to check for it)
                        dictionary_.addSymbol(token->normalizedText(), Dictionary::CORRELATION_NAME);

                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_FROM_DOT2:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident3_ = token;
                    parseState_ = SAW_FROM_IDENT3;
                }
                else
                {
                    // this is actually a SQL syntax error; but let's assume ident1_ is a schema name
                    // and ident2_ is an object name
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::SCHEMA_NAME);
                    ident1_ = 0;
                    dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident2_ = 0;
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_FROM_IDENT3:
            {
                // at this point, we know we have ident1.ident2.ident3, so we can assume ident1_
                // is a catalog name, ident2_ is a schema name and ident3_ is an object name
                dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::CATALOG_NAME);
                ident1_ = 0;
                dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::SCHEMA_NAME);
                ident2_ = 0;
                dictionary_.addSymbol(ident3_->normalizedText(), Dictionary::OBJECT_NAME);
                ident3_ = 0;
                if (token->type() == Token::TOKEN_IDENTIFIER)
                    // there's another identifier after it; must be a correlation name
                    // (note that we treat AS as whitespace, so we don't have to check for it)
                    dictionary_.addSymbol(token->normalizedText(), Dictionary::CORRELATION_NAME);

                parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_TABLE:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident1_ = token;
                    parseState_ = SAW_TABLE_IDENT1;
                }
                else
                {
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_TABLE_IDENT1:
            {
                if (token->type() == Token::TOKEN_DOT)
                    parseState_ = SAW_TABLE_DOT1;
                else
                {
                    // must be ident1_ is a table name context (could be view or index name)
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident1_ = 0;
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_TABLE_DOT1:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident2_ = token;
                    parseState_ = SAW_TABLE_IDENT2;
                }
                else
                {
                    // this is actually a SQL syntax error; but let's assume ident1_ is an object name
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident1_ = 0;
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_TABLE_IDENT2:
            {
                if (token->type() == Token::TOKEN_DOT)
                    parseState_ = SAW_TABLE_DOT2;
                else
                {
                    // must be ident1_ is a schema name
                    // must be ident2_ is a table name context (could be view)
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::SCHEMA_NAME);
                    ident1_ = 0;
                    dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident2_ = 0;
                    parseState_ = NOTHING_SPECIAL;
                }
                break;
            }
            case SAW_TABLE_DOT2:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    ident3_ = token;
                    // at this point, we know we have ident1.ident2.ident3, so we can assume ident1_
                    // is a catalog name, ident2_ is a schema name and ident3_ is an object name
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::CATALOG_NAME);
                    ident1_ = 0;
                    dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::SCHEMA_NAME);
                    ident2_ = 0;
                    dictionary_.addSymbol(ident3_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident3_ = 0;
                }
                else
                {
                    // this is actually a SQL syntax error; but let's assume ident1_ is a schema name
                    // and ident2_ is an object name
                    dictionary_.addSymbol(ident1_->normalizedText(), Dictionary::SCHEMA_NAME);
                    ident1_ = 0;
                    dictionary_.addSymbol(ident2_->normalizedText(), Dictionary::OBJECT_NAME);
                    ident2_ = 0;
                }
                parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_CONTROL:
            {
                if (token->type() == Token::TOKEN_QUERY)
                    parseState_ = SAW_CONTROL_QUERY;
                else if (token->type() == Token::TOKEN_TABLE)
                    parseState_ = SAW_TABLE;
                else
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_CONTROL_QUERY:
            {
                if (token->type() == Token::TOKEN_DEFAULT)
                    parseState_ = SAW_CONTROL_DEFAULT;
                else
                    parseState_ = NOTHING_SPECIAL;  // might be CONTROL QUERY SHAPE for example
                break;
            }
            case SAW_CONTROL_DEFAULT:
            {
                if (token->type() == Token::TOKEN_IDENTIFIER)
                {
                    // don't treat CQD names as identifiers; that way they won't
                    // be obfuscated
                    token->setType(Token::TOKEN_CQDNAME);
                    parseState_ = SAW_CONTROL_IDENT;
                }
                else
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_CONTROL_IDENT:
            {
                if (token->type() == Token::TOKEN_STRING_CONSTANT)
                {
                    // don't treat CQD values as strings; that way they won't
                    // be obfuscated
                    token->setType(Token::TOKEN_OTHER);
                    parseState_ = NOTHING_SPECIAL;
                }
                else
                    parseState_ = NOTHING_SPECIAL; // probably RESET keyword

                break;
            }
            case SAW_SIZED_DATATYPE:
            {
                if (token->type() == Token::TOKEN_PARENTHESIS)
                    parseState_ = SAW_SD_LPAREN;
                else  // TODO: Check for TIME, TIMESTAMP, DATE literals and set context for them
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_SD_LPAREN:
            {
                if (token->type() == Token::TOKEN_NUMERIC_CONSTANT)
                {
                    parseState_ = SAW_SD_SIZE;
                    token->setType(Token::TOKEN_OTHER);  // suppress obfuscation of datatype sizes
                }
                else  // probable SQL syntax error
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_SD_SIZE:
            {
                if (token->type() == Token::TOKEN_COMMA)
                    parseState_ = SAW_SD_LPAREN;
                else // probably a right paren
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_SCHEMA:
            {
                // For now, assume that any name after the SCHEMA keyword is
                // a schema name. It could be catalog.schema though.
                if (token->type() == Token::TOKEN_IDENTIFIER)
                    dictionary_.addSymbol(token->normalizedText(), Dictionary::SCHEMA_NAME);

                parseState_ = NOTHING_SPECIAL;
                // TODO: Allow for catalog.schema
                break;
            }
            case SAW_NONOBFUSCATED_COMMAND:
            {
                // Convert all tokens to OTHER until we hit SEMICOLON so they won't be
                // obfuscated
                if (token->type() == Token::TOKEN_SEMICOLON)
                    parseState_ = NOTHING_SPECIAL;
                else
                    token->setType(Token::TOKEN_OTHER);
                break;
            }
            case SAW_ORDER_OR_GROUP:
            {
                if (token->type() == Token::TOKEN_BY)
                    parseState_ = SAW_ORDER_BY_OR_GROUP_BY;
                else
                    parseState_ = NOTHING_SPECIAL;
                break;
            }
            case SAW_ORDER_BY_OR_GROUP_BY:
            {
                // for ORDER BY n and GROUP BY n, don't obfuscate the 'n' since it
                // is a column position, not data
                if (token->type() == Token::TOKEN_NUMERIC_CONSTANT)
                    token->setType(Token::TOKEN_OTHER);
                parseState_ = NOTHING_SPECIAL;
            }
            default:
            {
                // shouldn't happen, but just put us back in NOTHING_SPECIAL state
                parseState_ = NOTHING_SPECIAL;
                break;
            }
            }  // end of switch

            if ((token->type() == Token::TOKEN_NUMERIC_CONSTANT) ||
                (token->type() == Token::TOKEN_STRING_CONSTANT))
            {
                dictionary_.addSymbol(token->normalizedText(), 0);
            }

            // maintain command state

            if (token->type() != Token::TOKEN_DIRECTIVE)  // don't change command state on a directive
                commandState_ = (token->type() == Token::TOKEN_SEMICOLON);

        }   // if not whitespace

        classifiedTokenList_.push_back(token);
    }

    return classifiedTokenList_;
}
