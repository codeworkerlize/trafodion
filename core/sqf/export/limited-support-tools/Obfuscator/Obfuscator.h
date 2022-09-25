// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#pragma once
//
//  Obfuscator.h
//
//  This file defines the Obfuscator object. This object contains all
//  the state of the SQL Obfuscator program.
//

#include <string>

using namespace std;

class Obfuscator
{
public:

    Obfuscator(const string & inputFile,
        const string & outputFile,
        const string & inputDictionary,
        const string & outputDictionary,
        bool obfuscateConstants,
        bool obfuscateIdentifiers,
        bool encounterOrder);

    ~Obfuscator(void);

    // returns true if successful, false if errors
    bool run(void);

private:

    // the input file containing the SQL text to be obfuscated
    string inputFile_;

    // the output file where obfuscated SQL text should be written
    // if empty, no obfuscated SQL text is written
    string outputFile_;

    // the input dictionary containing substitutions for symbols
    // if empty, the Obfuscator generates its own
    string inputDictionary_;

    // the output dictionary; if empty, no dictionary is written
    string outputDictionary_;

    // controls whether constants are obfuscated
    bool obfuscateConstants_;

    // controls whether identifiers are obfuscated
    bool obfuscateIdentifiers_;

    // controls the order in which replacement symbols are generated
    bool encounterOrder_;

};
