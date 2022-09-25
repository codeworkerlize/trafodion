// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Dictionary.cpp
//
//  This file implements a dictionary for use by the SQL Obfuscation
//  program.
//

#include <iostream>
#include <fstream>
#include <cstdio>
#include <ctype.h>

using namespace std;

#include "Dictionary.h"

char * chop(char * buffer, char * & out)
{
    char * next = buffer;
    out = 0;  // assume failure

    // step over any leading white space

    while (isspace(*next))
        next++;

    // chop off a symbol from the buffer

    // the symbol is either a quoted string or a sequence of
    // non-blank characters

    if ((*next == '\'') || (*next == '"'))
    {
        // quoted string
        char quote = *next;
        out = next;
        next++;  // step over leading quote

        bool keepGoing = true;
        while (keepGoing)
        {
            if (*next == '\0')
                keepGoing = false;
            else if (*next == quote)
            {
                next++;
                if (*next == quote)
                    next++;  // escaped quote inside of string
                else
                    keepGoing = false;
            }
            else
                next++;
        }

        // next now points to the first byte after the trailing quote

    }
    else if (*next)
    {
        out = next;
        while ((*next) && (!isspace(*next)))
            next++;
    }

    if (out)
    {
        if (*next) // if there is whitespace
        {
            *next = '\0';  // overwrite with a null terminator
            next++;  // step over what was a whitespace character
        }
        // else already null-terminated
    }

    return next;
}



Dictionary::Dictionary(const string & inputDictionary, 
    bool obfuscateConstants,
    bool obfuscateIdentifiers,
    bool useEncounterOrder) 
    : constructorHadErrors_(false),
    obfuscateConstants_(obfuscateConstants),
    obfuscateIdentifiers_(obfuscateIdentifiers),
    useEncounterOrder_(useEncounterOrder),
    columnCounter_(1),
    catalogCounter_(1),
    schemaCounter_(1),
    objectCounter_(1),
    correlationCounter_(1),
    numberCounter_(1),
    stringCounter_(1)
{
    if (inputDictionary.size())  // if there is an input dictionary
    {
        ifstream dictionaryFile(inputDictionary);

        if (!dictionaryFile)
        {
            cout << "Could not open input dictionary file " << inputDictionary << "." << endl;
            constructorHadErrors_ = true;
        }
        else
        {
            // read the input dictionary
            // 
            // expected format of each line: <symbol> <replacement>
            //
            // <symbol> and <replacement> are either quoted strings or
            // strings of non-whitespace characters

            int lineCount = 0;
            char buffer[4096+1];
            while ((dictionaryFile.is_open()) && (dictionaryFile.good()))
            {
                lineCount++;
                dictionaryFile.getline(buffer, sizeof(buffer) - 1, '\n');
                if (dictionaryFile.eof())
                {
                    dictionaryFile.close();
                }
                else if  ((dictionaryFile.fail()) || (dictionaryFile.bad()))
                {
                    cout << "Error encountered when reading dictionary file " << inputDictionary << "." << endl;
                    dictionaryFile.close();
                    constructorHadErrors_ = true;
                }
                else  // got a line
                {
                    char * symbol = 0;
                    char * replacement = 0;

                    char * next = chop(buffer, symbol /* out */);
                    next = chop(next, replacement /* out */);

                    if ((symbol != 0) && (replacement != 0))
                    {
                        string symbolStr = symbol;
                        string replacementStr = replacement;
                        addSymbol(symbolStr, replacementStr);
                    }
                    else
                    {
                        cout << "Invalid line encountered at line " << lineCount << " in input dictionary " <<
                            inputDictionary << "." << endl;
                        constructorHadErrors_ = true;
                    }
                }
            }
        }
    }
}

Dictionary::~Dictionary(void)
{

}

void Dictionary::addSymbol(const string & symbol, unsigned int context)
{
    // there are a few well-known names that it is better not to obfuscate
    // e.g., TRAFODION, HIVE, HBASE catalog names

    if (doNotObfuscate(symbol, context))
        context |= DO_NOT_OBFUSCATE;

    // if the symbol is already in the map, just OR the context parameter
    // with its saved context value; otherwise create a new entry

    map<string, DictionaryEntry>::iterator it = symbolTable_.find(symbol);
    if (it == symbolTable_.end())  // if not found
    {
        symbolTable_.insert(pair<string, DictionaryEntry>(symbol, { string(""), context }));
        encounterOrder_.push_back(symbol);
    }
    else
    {
        DictionaryEntry & entry = it->second;
        entry.context_ |= context;
    }
}

void Dictionary::addSymbol(const string & symbol, const string & replacement)
{
    // this variant of addSymbol is used only by the constructor; it expects
    // that a given symbol is always new to the symbol table

    bool useReplacement = true;  // assume we'll use the replacement
    if (isalpha(symbol[0]) || (symbol[0] == '"'))  // if symbol is an identifier
    {
        if (!obfuscateIdentifiers_)  // if we aren't ask to obfuscate identifiers
            useReplacement = false;  // then don't store a replacement
    }
    else  // it's a literal constant
    {
        if (!obfuscateConstants_)  // if we aren't ask to obfuscate constants
            useReplacement = false;  // then don't store a replacement
    }

    map<string, DictionaryEntry>::iterator it = symbolTable_.find(symbol);
    if (it == symbolTable_.end())  // if not found
    {
        if (useReplacement)
            symbolTable_.insert(pair<string, DictionaryEntry>(symbol, { replacement, 0 }));
        else
        {
            string emptyString;
            symbolTable_.insert(pair<string, DictionaryEntry>(symbol, { emptyString, 0 }));
        }
    }
    else
    {
        cout << "The symbol " << symbol << " occurs more than once in the input dictionary." << endl;
        constructorHadErrors_ = true;
    }
}

bool Dictionary::doNotObfuscate(const string & symbol, unsigned int context)
{
    bool rc = false;
    if (context & CATALOG_NAME)
    {
        if (symbol == "TRAFODION")
            rc = true;
        else if (symbol == "HIVE")
            rc = true;
        else if (symbol == "HBASE")
            rc = true;
    }
    else if (context & SCHEMA_NAME)
    {
        if (symbol == "_ROW_")
            rc = true;
        else if (symbol == "_CELL_")
            rc = true;
        else if (symbol == "_MD_")
            rc = true;
        else if (symbol == "_REPOS_")
            rc = true;
        else if (symbol == "_PRIVMGR_")
            rc = true;
    }
    else if (context & OBJECT_NAME)
    {
        if (symbol == "SB_HISTOGRAMS")
            rc = true;
        else if (symbol == "SB_HISTOGRAM_INTERVALS")
            rc = true;
    }

    return rc;
}

const string * Dictionary::getReplacement(const string & symbol)
{
    const string * result = 0;  // assume not found
    map<string, DictionaryEntry>::iterator it = symbolTable_.find(symbol);
    if (it != symbolTable_.end())  // if found
    {
        DictionaryEntry & entry = it->second;
        if (((entry.context_ & DO_NOT_OBFUSCATE) == 0) &&
            ((&entry.replacement_)->size() > 0))
            result = &entry.replacement_;
    }

    return result;
}

void Dictionary::generateReplacement(map<string, DictionaryEntry>::iterator & it)
{
    DictionaryEntry & entry = it->second;
    const string & symbol = it->first;
    if ((entry.replacement_.size() == 0) && ((entry.context_ & DO_NOT_OBFUSCATE) == 0))
    {
        char buffer[20];
        if ((isalpha(symbol[0]) || (symbol[0] == '"')) &&  // obfuscate identifiers if desired
            obfuscateIdentifiers_)
        {
            if (entry.context_ & OBJECT_NAME)
            {
                sprintf(buffer, "T%d", objectCounter_);
                objectCounter_++;
            }
            else if (entry.context_ & CORRELATION_NAME)
            {
                sprintf(buffer, "X%d", correlationCounter_);
                correlationCounter_++;
            }
            else if (entry.context_ & SCHEMA_NAME)
            {
                sprintf(buffer, "SCH%d", schemaCounter_);
                schemaCounter_++;
            }
            else if (entry.context_ & CATALOG_NAME)
            {
                sprintf(buffer, "CAT%d", catalogCounter_);
                catalogCounter_++;
            }
            else
            {
                // assume it is a column name
                sprintf(buffer, "C%d", columnCounter_);
                columnCounter_++;
            }
            entry.replacement_ = buffer;
        }
        else if (obfuscateConstants_)  // it's a constant, obfuscate if desired
        {
            if (isdigit(symbol[0]))  // if it's a number
            {
                sprintf(buffer, "%d", numberCounter_);
                numberCounter_++;
            }
            else  // it's a string
            {
                // TODO: Later distinguish between timestamps, dates and strings
                // based on a context setting passed by the Classifier
                sprintf(buffer, "'string%d'", stringCounter_);
                stringCounter_++;
            }
            entry.replacement_ = buffer;
        }
    }
}


void Dictionary::generateReplacements(void)
{
    if (useEncounterOrder_)
    {
        // assign obfuscated symbols in the order in which we first encountered them
        map<string, DictionaryEntry>::iterator it;
        for (size_t i = 0; i < encounterOrder_.size(); i++)
        {
            it = symbolTable_.find(encounterOrder_[i]);
            generateReplacement(it);
        }
    }
    else
    {
        // assign obfuscated symbols in lexicographical order

        for (map<string, DictionaryEntry>::iterator it = symbolTable_.begin();
        it != symbolTable_.end();
            it++)
        {
            generateReplacement(it);         
        }
    }
}


bool Dictionary::writeDictionary(const string & outputFile)
{
    bool rc = true;  // assume success

    ofstream out(outputFile);

    if (!out)
    {
        cout << "Could not open dictionary output file " << outputFile << "." << endl;
        rc = false;
    }
    else
    {
        for (map<string, DictionaryEntry>::iterator it = symbolTable_.begin();
            it != symbolTable_.end();
            it++)
        {
            const string & symbol = it->first;
            DictionaryEntry & entry = it->second;

            // only write out symbols having replacements

            if (entry.replacement_.size() > 0)
            {
                out << symbol << "    " << entry.replacement_ << endl;
            }
        }
        out.close();
    }

    return rc;
}
