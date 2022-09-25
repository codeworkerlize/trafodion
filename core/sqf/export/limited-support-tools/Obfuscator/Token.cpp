// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Token.cpp
//
//  This file implements the Token object.
//

#include <iostream>
#include <string>
#include <map>
#include <ctype.h>
#include <initializer_list>

#include "Token.h"

using namespace std;


Token::Token(TokenType t, const char * start, const char * end) : type_(t), originalText_(start,end-start)
{
    // for identifiers, normalize the text
    normalizeIdentifier();

    // if the token is an identifier and happens to be a SQL keyword,
    // change the token type
    type_ = keywordOrIdentifier();
}

Token::~Token()
{
    // nothing to do
}

void Token::setType(TokenType t)
{
    type_ = t;
}

const string & Token::originalText(void) const
{
    return originalText_;
}

const string & Token::normalizedText(void) const
{
    if (normalizedText_.size() > 0)
        return normalizedText_;

    return originalText_;
}

void Token::display(void) const
{
    if (normalizedText_.size())
        cout << "Token type " << type_ << ": " << normalizedText_ << endl;
    else
        cout << "Token type " << type_ << ": " << originalText_ << endl;
}

void Token::normalizeIdentifier(void)
{
    // if it is an identifier token and is a regular identifier,
    // normalize the text by upshifting it

    // if it is an identifier token and is a delimited identifier,
    // and if its text is a valid regular identifier and all in
    // upper case, then normalize the text by removing the surrounding
    // double quotes

    if (type_ == TOKEN_IDENTIFIER)
    {
        if (originalText_[0] == '"')
        {
            // delimited identifier case
            char * temp = new char[originalText_.size()];
            char * tempNext = temp;
            const char * next = originalText_.c_str() + 1;  // step over leading double quote
            const char * last = originalText_.c_str() + originalText_.size() - 1;
            bool good = true;
            if (isalpha(*next))  // we know identifier is at least 2 chars so don't need to check for end
            {
                *tempNext = *next;
                tempNext++;
                next++;
            }
            else
            {
                good = false;  // doesn't start with an alpha
            }
            while ((next < last) && (good))
            {
                if (isalnum(*next) || (*next == '_'))
                {
                    if (toupper(*next) == *next)
                    {
                        *tempNext = *next;
                        tempNext++;
                        next++;
                    }
                    else
                        good = false;  // has a lower case alpha
                }
                else
                {
                    good = false;  // has something besides an alphanumeric or underscore
                }
            }
            *tempNext = '\0';

            if (good)
                normalizedText_ = temp;

            delete temp;
        }
        else
        {
            // regular identifier case
            normalizedText_ = originalText_;
            for (size_t i = 0; i < normalizedText_.size(); i++)
            {
                normalizedText_[i] = toupper(normalizedText_[i]);
            }
        }
    }
}

// TODO: Do a thorough check of all the tokens in the SQL parser, and 
// add any reserved words here.

static map<string, Token::TokenType> keywordMap =
{
    { "ALL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ALIGNED", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ALTER", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "AND", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "AS", Token::TOKEN_WHITESPACE } ,  // AS can be omitted without changing semantics; treat as white space
    { "ASC", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ATTRIBUTES", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "AVG", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "BETWEEN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "BY", Token::TOKEN_BY } ,
    { "CASE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CAST", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CHANGED", Token::TOKEN_SIZED_DATATYPE } ,
    { "CHAR", Token::TOKEN_SIZED_DATATYPE } ,
    { "CHARACTER", Token::TOKEN_SIZED_DATATYPE } ,
    { "CHECK", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CREATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CROSS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "COLUMN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "COLLATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CONTROL", Token::TOKEN_CONTROL } ,
    { "CONSTRAINT", Token::TOKEN_CONTROL } ,
    { "COUNT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CQD", Token::TOKEN_CQD } ,
    { "CURRENT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CURRENT_DATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "CURRENT_TIMESTAMP", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DATA_BLOCK_ENCODING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DECIMAL", Token::TOKEN_SIZED_DATATYPE } ,
    { "DEFAULT", Token::TOKEN_DEFAULT } ,
    { "DELETE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DENSE_RANK", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DESC", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DIFF1", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DIFF2", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DISTINCT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DROP", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "DROPPABLE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ELSE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "END", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "EVERY", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "EXECUTE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "EXISTS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "EXPLAIN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FIRST", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FLOAT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FOLLOWING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FOR", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FOREIGN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FORMAT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "FROM", Token::TOKEN_FROM } ,
    { "GENERATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "GROUP", Token::TOKEN_ORDER_OR_GROUP } ,
    { "HASH", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "HAVING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "HBASE_OPTIONS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "HEADING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "IN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INCLUSIVE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INDEX", Token::TOKEN_TABLE } ,  // treat INDEX like TABLE (that is, an object name likely follows)
    { "INSERT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INTEGER", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INTERVAL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INTERVALS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "INTO", Token::TOKEN_TABLE } , // treat INTO like TABLE (that is, an object name likely follows) -- can there be correlation?
    { "IS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ISO88591", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "JOIN", Token::TOKEN_FROM } ,  // treat JOIN like FROM (both can have object names and correlation names after them)
    { "KEY", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LARGEINT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LAST", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LASTNOTNULL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LEFT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LIKE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LOAD", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LOCATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "LOG", Token::TOKEN_NONOBFUSCATED_COMMAND_VERB },
    { "LPAD", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "NATURAL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "NUMERIC", Token::TOKEN_SIZED_DATATYPE } ,
    { "MEMSTORE_FLUSHSIZE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MERGE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MAX", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MIN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOD", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGAVG", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGCOUNT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGMAX", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGMIN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGRANK", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGSTDDEV", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGSUM", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "MOVINGVARIANCE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "NO", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "NOT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "NULL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "OBEY", Token::TOKEN_NONOBFUSCATED_COMMAND_VERB } ,
    { "OFFSET", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ON", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "OR", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ORDER", Token::TOKEN_ORDER_OR_GROUP } ,
    { "OUTER", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "OVER", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "QUERY", Token::TOKEN_QUERY } ,
    { "PARAM", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "PARTITION", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "PARTITIONS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "POSITION", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "PRECEDING", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "PREPARE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "PRIMARY", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RANK", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RESET", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RIGHT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "ROW", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "ROW_NUMBER", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "ROWS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGAVG", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGCOUNT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGMAX", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGMIN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGRANK", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "RUNNINGSTDDEV", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "RUNNINGSUM", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "RUNNINGVARIANCE", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "SALT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SCHEMA", Token::TOKEN_SCHEMA } ,
    { "SELECT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SEQUENCE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SERIALIZED", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SHAPE", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "SET", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SH", Token::TOKEN_NONOBFUSCATED_COMMAND_VERB },
    { "SHAPE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SHOWCONTROL", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SINCE", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "SMALLINT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SOME", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SUBSTRING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "SUM", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "STATISTICS", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "STDDEV", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "STORE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "TABLE", Token::TOKEN_TABLE } ,
    { "THEN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "THIS", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "TIME", Token::TOKEN_TIME } ,
    { "TIMESTAMP", Token::TOKEN_TIMESTAMP },
    { "TRANSPOSE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "TRIM", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "UTF8", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "UNION", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "UNBOUNDED", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "UNSIGNED", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "UPDATE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "UPSERT", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "UNIQUE", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "USING", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "VALUES", Token::TOKEN_OTHER_SQL_KEYWORD } ,
    { "VARCHAR", Token::TOKEN_SIZED_DATATYPE } ,
    { "VARIANCE", Token::TOKEN_SIZED_DATATYPE },
    { "VARYING", Token::TOKEN_SIZED_DATATYPE } ,
    { "VIEW", Token::TOKEN_TABLE } ,  // treat VIEW like TABLE (that is, an object name likely follows)
    { "VOLATILE", Token::TOKEN_OTHER_SQL_KEYWORD },
    { "WHEN", Token::TOKEN_OTHER_SQL_KEYWORD } ,
{ "WHERE", Token::TOKEN_OTHER_SQL_KEYWORD } 
};

Token::TokenType Token::keywordOrIdentifier(void)
{
    TokenType result = type_;

    if ((type_ == TOKEN_IDENTIFIER) && 
        (originalText_[0] != '"'))  // only consider regular identifiers 
    {
        map<string, TokenType>::iterator it = keywordMap.find(normalizedText_);
        if (it != keywordMap.end())
        {
            result = it->second;
        }
    }
    return result;
}
