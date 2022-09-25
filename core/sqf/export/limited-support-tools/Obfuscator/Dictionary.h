// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#pragma once
//
//  Dictionary.h
//
//  This file defines a dictionary for use by the SQL Obfuscation
//  program.
//

#include <string>
#include <map>
#include <vector>

using namespace std;

class Dictionary
{
public:

    Dictionary(const string & inputDictionary,
        bool obfuscateConstants,
        bool obfuscateIdentifiers,
        bool useEncounterOrder);

    ~Dictionary(void);

    inline bool constructorHadErrors(void)
    {
        return constructorHadErrors_; 
    }

    enum { CATALOG_NAME = 1, 
        SCHEMA_NAME = 2, 
        OBJECT_NAME = 4, 
        CORRELATION_NAME = 8, 
        DO_NOT_OBFUSCATE = 16 };

    // context is the bitwise OR of values from the enum above
    void addSymbol(const string & symbol, unsigned int context);

    // returns null if the symbol is not found
    const string * getReplacement(const string & symbol);

    void generateReplacements(void);

    bool writeDictionary(const string & outputFile);

private:

    // used only by Dictionary constructor when reading input dictionary
    void addSymbol(const string & symbol, const string & replacement);

    // used to detect names that should not be obfuscated
    bool doNotObfuscate(const string & symbol, unsigned int context);

    struct DictionaryEntry
    {
        string replacement_;
        unsigned int context_;
    };

    // generate obfuscated symbol
    void generateReplacement(map<string, DictionaryEntry>::iterator & it);

    bool constructorHadErrors_;

    // true if we are to obfuscate constants
    bool obfuscateConstants_;

    // true if we are to obfuscate identifiers
    bool obfuscateIdentifiers_;

    // true if we want to assign obfuscated values in the order we encounter
    // the symbols; false if we want to assign them in lexicographical order
    bool useEncounterOrder_;

    map<string, DictionaryEntry> symbolTable_;

    vector<string> encounterOrder_;

    // state used by generateReplacement method

    unsigned int columnCounter_;
    unsigned int catalogCounter_;
    unsigned int schemaCounter_;
    unsigned int objectCounter_;
    unsigned int correlationCounter_;

    unsigned int numberCounter_;
    unsigned int stringCounter_;

};
