// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Scanner.cpp
//
//  This file defines the Scanner object. It scans the text
//  of a SQL statement, converting it into a stream of Tokens
//  that can be pipelined to the next pass. The scan is a
//  light-weight scan that is sufficient for our purposes;
//  we don't attempt to be equivalent to a production SQL
//  scanner.
//

#include <ctype.h>
#include <iostream>
#include <string.h>

using namespace std;

#include "Token.h"
#include "Scanner.h"

Scanner::Scanner(const string & inputFile) : hadErrors_(false), inputFile_(inputFile)
{
    if (!inputFile_)
    {
        cout << "Could not open input SQL text file " << inputFile << "." << endl;
        hadErrors_ = true;
    }

    currentBuffer_[0] = '\0';
    next_ = currentBuffer_;
}

Scanner::~Scanner(void)
{
    // nothing more to do; member dtors take care of it all
}

Token * Scanner::getNextToken(void)
{
    Token * result = 0;  // assume no more tokens

    if ((*next_ == '\0') && (inputFile_.is_open()) && (inputFile_.good()))
    {
        // unfortunately, the ifstream::get() function sets the fail bit
        // when a line consists of just '\n'; so check for this before
        // doing a get
        int firstChar = inputFile_.peek();
        if (firstChar == '\n')
        {
            firstChar = inputFile_.get();  // remove '\n' from stream
            currentBuffer_[0] = '\n';
            currentBuffer_[1] = '\0';
            next_ = currentBuffer_;
        }
        else if (firstChar == -1)  // end of file
        {
            inputFile_.close();
            currentBuffer_[0] = '\0';
        }
        else
        {
            inputFile_.get(currentBuffer_, CURRENT_BUFFER_SIZE, '\n');
            if (inputFile_.bad())
            {
                cout << "I/O error reading SQL text file." << endl;
                hadErrors_ = true;
                currentBuffer_[0] = '\0';  // insure buffer is empty
                inputFile_.close();
            }
            if (inputFile_.fail())
            {
                cout << "I/O failed reading SQL text file." << endl;
                hadErrors_ = true;
                currentBuffer_[0] = '\0';  // insure buffer is empty
                inputFile_.close();
            }
            else if (inputFile_.eof())
            {
                inputFile_.close();
            }
            else
            {
                size_t bytesRead = strlen(currentBuffer_);
                if (bytesRead < CURRENT_BUFFER_SIZE)
                {
                    // must have stopped on '\n'
                    int c = inputFile_.get();  // skip the '\n' so we can do another get above
                    currentBuffer_[bytesRead] = '\n';
                    currentBuffer_[bytesRead + 1] = '\0';
                }
            }
        }

        next_ = currentBuffer_;
    }

    if (*next_)
    {
        const char * start = next_;
        switch (*next_)
        {
        case '+':
        case '\\':
        case '*':
        case '=':
        {
            next_++;
            result = new Token(Token::TOKEN_SCALAR_OPERATOR, start, next_);
            break;
        }
        case '-':
        {
            if (next_[1] != '-')
            {
                next_++;
                result = new Token(Token::TOKEN_SCALAR_OPERATOR, start, next_);
            }
            else
            {
                // it's a SQL comment; treat it like white space
                next_ = next_ + strlen(next_);  // runs to the end of the line
                result = new Token(Token::TOKEN_WHITESPACE, start, next_);
            }
            break;
        }
        case '!':
        case '>':
        {
            next_++;
            if (*next_ == '=')
            {
                next_++;
            }
            result = new Token(Token::TOKEN_SCALAR_OPERATOR, start, next_);
            break;
        }
        case '<':
        {
            next_++;
            if ((*next_ == '=') || (*next_ == '>'))
            {
                next_++;
            }
            result = new Token(Token::TOKEN_SCALAR_OPERATOR, start, next_);
            break;
        }
        case '|':
        {
            next_++;
            if (*next_ == '|')
            {
                next_++;
            }
            result = new Token(Token::TOKEN_SCALAR_OPERATOR, start, next_);
            break;
        }
        case ')':
        case '(':
        {
            next_++;
            result = new Token(Token::TOKEN_PARENTHESIS, start, next_);
            break;
        }
        case '\'':   // beginning of string constant
        case '"':    // beginning of delimited identifier
        {
            char quote = *next_;
            next_++;  // step over opening quote
            bool keepGoing = true;

            while (keepGoing)
            {
                if (*next_ == '\0')
                    keepGoing = false;
                else if (*next_ == quote)
                {
                    next_++;
                    if (*next_ == quote)
                        next_++;  // escaped quote inside of string
                    else
                        keepGoing = false;
                }
                else
                    next_++;
            }

            if (*start == '\'')
                result = new Token(Token::TOKEN_STRING_CONSTANT, start, next_);
            else
                result = new Token(Token::TOKEN_IDENTIFIER, start, next_);

            break;
        }
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        {
            next_++;
            while (isdigit(*next_))
                next_++;
            if (*next_ == '.')
                next_++;
            while (isdigit(*next_))
                next_++;
            if ((*next_ == 'e') || (*next_ == 'E'))
                next_++;
            while (isdigit(*next_))
                next_++;

            result = new Token(Token::TOKEN_NUMERIC_CONSTANT, start, next_);
            break;
        }
        case '.':
        {
            next_++;
            result = new Token(Token::TOKEN_DOT, start, next_);
            break;
        }
        case ',':
        {
            next_++;
            result = new Token(Token::TOKEN_COMMA, start, next_);
            break;
        }
        case ';':
        {
            next_++;
            result = new Token(Token::TOKEN_SEMICOLON, start, next_);
            break;
        }
        case '?':
        {
            // ? might be a SQL dynamic parameter or the beginning of a named parameter;
            // if it is at the beginning of the line it might be a ?section sqlci statement

            bool isSection = false;
            if (next_ == currentBuffer_)  // if at beginning of the line
            {
                char temp[1 + 7 + 1 + 1];  // room for "?section " and null terminator
                strncpy(temp, next_, 1+7+1);
                temp[1 + 7 + 1] = '\0';
                for (size_t i = 1; i < 1 + 7 + 1; i++)
                {
                    temp[i] = tolower(temp[i]);
                }
                if (strcmp(temp, "?section ") == 0)
                    isSection = true;
            }

            if (isSection)
            {
                next_ = next_ + strlen(next_);  // take it to end of line
                result = new Token(Token::TOKEN_DIRECTIVE, start, next_);
            }
            else
            {
                next_++;
                result = new Token(Token::TOKEN_OTHER, start, next_);
            }
            break;
        }
        case '#':  // possible sqlci #ifdef, #else, #endif
        {
            bool isDirective = false;
            if (next_ == currentBuffer_)  // if at beginning of the line
            {
                char temp[1 + 5 + 1];  // room for "?ifdef " and null terminator
                strncpy(temp, next_, 1 + 5);
                temp[1 + 5] = '\0';
                for (size_t i = 1; i < 1 + 5; i++)
                {
                    temp[i] = tolower(temp[i]);
                }
                if ((strcmp(temp, "#ifdef") == 0) ||
                    (strcmp(temp, "#ifnsk") == 0) ||
                    (strcmp(temp, "#endif") == 0))
                    isDirective = true;
                else
                {
                    temp[1 + 4] = '\0';
                    if ((strcmp(temp, "#else") == 0) ||
                        (strcmp(temp, "#ifmp") == 0) ||
                        (strcmp(temp, "#ifmx") == 0))
                        isDirective = true;
                }
            }

            if (isDirective)
            {
                next_ = next_ + strlen(next_);  // take it to end of line
                result = new Token(Token::TOKEN_DIRECTIVE, start, next_);
            }
            else
            {
                next_++;
                result = new Token(Token::TOKEN_OTHER, start, next_);
            }
            break;
        }
        default:
        {
            if (isspace(*next_))
            {
                next_++;
                while (isspace(*next_))
                    next_++;
                result = new Token(Token::TOKEN_WHITESPACE, start, next_);
            }
            else if (isalpha(*next_))
            {
                next_++;
                while ((isalnum(*next_)) || (*next_ == '_'))
                    next_++;
                // Token constructor will generate appropriate token
                // if the identifier text happens to be a SQL keyword
                result = new Token(Token::TOKEN_IDENTIFIER, start, next_);
            }
            else
            {
                next_++;
                result = new Token(Token::TOKEN_OTHER, start, next_);
                break;
            }
        }
        } // end of switch 
    }

    return result;
}




