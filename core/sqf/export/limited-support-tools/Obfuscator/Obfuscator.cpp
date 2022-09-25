// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  Obfuscator.cpp
//
//  This file implements the Obfuscator object. This object contains all
//  the state of the SQL Obfuscator program and drives its processing.
//

#include "Obfuscator.h"
#include "Dictionary.h"
#include "Scanner.h"
#include "Token.h"
#include "Classifier.h"

#include <list>
#include <iostream>
#include <fstream>

using namespace std;

Obfuscator::Obfuscator(const string & inputFile,
    const string & outputFile,
    const string & inputDictionary,
    const string & outputDictionary,
    bool obfuscateConstants,
    bool obfuscateIdentifiers,
    bool encounterOrder)
    : inputFile_(inputFile), outputFile_(outputFile),
    inputDictionary_(inputDictionary), outputDictionary_(outputDictionary),
    obfuscateConstants_(obfuscateConstants), obfuscateIdentifiers_(obfuscateIdentifiers),
    encounterOrder_(encounterOrder)
{
    // nothing else to do for the moment
}

Obfuscator::~Obfuscator(void)
{
    // nothing to do
}

bool Obfuscator::run(void)
{
    bool rc = true;   // assume success

    // Pseudo-code:
    //
    // Create initial dictionary (empty, or populated from inputDictionary_)
    // Read input file into a list of tokens
    // Link tokens to dictionary entries where appropriate (and creating
    // entries as needed) -- can be done with read step
    // Pass through the token list classifying the tokens by their context,
    // updating their dictionary entries -- can be done with read step
    // Pass through the dictionary assigning replacements -- separate pass
    // Pass through the token list generating output -- separate pass
    // Pass through the dictionary writing out the generated replacements

    Dictionary dictionary(inputDictionary_, obfuscateConstants_, obfuscateIdentifiers_, encounterOrder_);

    if (dictionary.constructorHadErrors())
    {
        rc = false;  // failure
    }   
    else
    {
        Scanner scanner(inputFile_);
        Classifier classifier(scanner,dictionary);

        // read the input file into a list of tokens, classify them as 
        // needed, and enter symbols into the dictionary

        list<const Token *> classifiedTokens = classifier.classify();

        // assign replacement symbols

        dictionary.generateReplacements();

        // generate obfuscated output if desired

        if (outputFile_.size())
        {
            ofstream outputStream(outputFile_);
            if (!outputStream)
            {
                cout << "Could not open output file " << outputFile_ << "." << endl;
                rc = false;
            }
            else
            {
                for (list<const Token *>::iterator it = classifiedTokens.begin();
                it != classifiedTokens.end();
                    it++)
                {
                    const Token * token = *it;
                    const string * replacement = 0;

                    if ((token->type() == Token::TOKEN_IDENTIFIER) ||
                        (token->type() == Token::TOKEN_NUMERIC_CONSTANT) ||
                        (token->type() == Token::TOKEN_STRING_CONSTANT))
                    {
                        replacement = dictionary.getReplacement(token->normalizedText());
                    }
                    
                    if (replacement)
                    {
                        outputStream << *replacement;
                    }
                    else
                    {
                        outputStream << token->originalText();
                    }
                }
                outputStream.close();
            }
        }

        // write out dictionary if desired

        if (outputDictionary_.size())
        {
            rc = rc && dictionary.writeDictionary(outputDictionary_);
        }

        // debugging code
   /*     for (list<const Token *>::iterator it = classifiedTokens.begin();
        it != classifiedTokens.end();
            it++)
        {
            const Token * token = *it;
            token->display();
        } */

    }

    return rc;
}
