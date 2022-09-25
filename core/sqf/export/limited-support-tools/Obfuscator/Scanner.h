// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Scanner.h
//
//  This file defines the Scanner object. It scans the text
//  of a SQL statement, converting it into a stream of Tokens
//  that can be pipelined to the next pass.
//

#include <string>
#include <fstream>

using namespace std;

// forward reference
class Token;  


class Scanner
{
public:

    Scanner(const string & inputFile);
    ~Scanner();

    // returns null if there are no more tokens; the
    // caller can check hadErrors() to see if errors
    // occurred
    Token * getNextToken(void);

    inline bool hadErrors(void)
    {
        return hadErrors_;
    }

private:

    bool hadErrors_;
    ifstream inputFile_;

    const char * next_;  // points somewhere in currentBuffer_

    enum { CURRENT_BUFFER_SIZE = 4096 };
    char currentBuffer_[CURRENT_BUFFER_SIZE + 1];

};

