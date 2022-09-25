// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

//
//  ObfuscatorMain.cpp
// 
//  This is the main of the SQL Obfuscator program.
//

#include <iostream>
#include <string>
#include <string.h>

using namespace std;

#include "Obfuscator.h"

bool processArguments(int argc,
    char * argv[],
    string & inputFile,
    string & outputFile,
    string & inputDictionary,
    string & outputDictionary,
    bool & obfuscateConstants,
    bool & obfuscateIdentifiers,
    bool & encounterOrder)
{
    bool rc = true;  // assume good arguments
    bool helpRequested = false;

    for (int i = 1; i < argc; i++)  // argv[0] is the executable name; skip over it
    {
        if (strcmp(argv[i], "-i") == 0)
        {
            if (inputFile.size() > 0)
            {
                cout << argv[i] << " parameter is specified twice. It may be specified only once." << endl;
                rc = false;
            }
            else if ((i + 1 == argc) || (argv[i + 1][0] == '-'))
            {
                cout << "Missing file name after " << argv[i] << "." << endl;
                rc = false;
            }
            else
            {
                inputFile = argv[i + 1];
                i++;
            }
        }
        else if (strcmp(argv[i], "-o") == 0)
        {
            if (outputFile.size() > 0)
            {
                cout << argv[i] << " parameter is specified twice. It may be specified only once." << endl;
                rc = false;
            }
            else if ((i + 1 == argc) || (argv[i + 1][0] == '-'))
            {
                cout << "Missing file name after " << argv[i] << "." << endl;
                rc = false;
            }
            else
            {
                outputFile = argv[i + 1];
                i++;
            }
        }
        else if (strcmp(argv[i], "-u") == 0)
        {
            if (inputDictionary.size() > 0)
            {
                cout << argv[i] << " parameter is specified twice. It may be specified only once." << endl;
                rc = false;
            }
            else if ((i + 1 == argc) || (argv[i + 1][0] == '-'))
            {
                cout << "Missing file name after " << argv[i] << "." << endl;
                rc = false;
            }
            else
            {
                inputDictionary = argv[i + 1];
                i++;
            }
        }
        else if (strcmp(argv[i], "-d") == 0)
        {
            if (outputDictionary.size() > 0)
            {
                cout << argv[i] << " parameter is specified twice. It may be specified only once." << endl;
                rc = false;
            }
            else if ((i + 1 == argc) || (argv[i + 1][0] == '-'))
            {
                cout << "Missing file name after " << argv[i] << "." << endl;
                rc = false;
            }
            else
            {
                outputDictionary = argv[i + 1];
                i++;
            }
        }
        else if (strcmp(argv[i], "-c") == 0)
        {
            obfuscateConstants = true;  // don't care if it is specified twice
        }
        else if (strcmp(argv[i], "-x") == 0)
        {
            obfuscateIdentifiers = true;  // don't care if it is specified twice
        }
        else if (strcmp(argv[i], "-l") == 0)
        {
            encounterOrder = false;  // don't care if it is specified twice
        }
        else if (strcmp(argv[i], "-h") == 0)
        {
            rc = false;  // user wants help, cause Usage info to display
            helpRequested = true;
        }
        else
        {
            cout << "Unrecognized parameter:" << argv[i] << endl;
            rc = false;
        }
    }

    if (!helpRequested)
    {
        if (!obfuscateConstants && !obfuscateIdentifiers)
        {
            cout << "At least one of -c and -x must be specified." << endl;
            rc = false;
        }
        if (inputFile.size() == 0)
        {
            cout << "The -i parameter must be specified." << endl;
            rc = false;
        }
    }

    return rc;
}


int main(int argc, char * argv[])
{
    int rc = 0;  // assume success
    string inputFile;
    string outputFile;
    string inputDictionary;
    string outputDictionary;
    bool obfuscateConstants = false;
    bool obfuscateIdentifiers = false;
    bool encounterOrder = true;

    if (!processArguments(argc,argv,inputFile,outputFile,inputDictionary,
        outputDictionary,obfuscateConstants,obfuscateIdentifiers,encounterOrder))
    {
        cout << endl;
        cout << "Usage: " << argv[0] << " -i <input file> [-o <output file>] [-d <dictionary file>] [-u <dictionary file>] [-c] [-x] [-l]" << endl;
        cout << endl;
        cout << "This program, the Obfuscator, is used to obfuscate identifiers or literals or" << endl;
        cout << "both in SQL text." << endl;
        cout << endl;
        cout << "Parameters are as follows:" << endl;
        cout << "-i <input file> gives the name of the file containing SQL text to be" << endl;
        cout << "   obfuscated. This is a required parameter." << endl;
        cout << "-o <output file> gives the name of a file where obfuscated text should be" << endl;
        cout << "   written. If omitted, no obfuscated text is written." << endl;
        cout << "-d <dictionary> gives the name of a file where the set of substitutions to be" << endl;
        cout << "   used by the Obfuscator should be written. This is useful if the user wishes" << endl;
        cout << "   to tailor the substitutions before actually making them. It is also useful" << endl;
        cout << "   if one wishes to keep a record of the substitutions made. If omitted, this" << endl;
        cout << "   file is not written." << endl;
        cout << "-u <dictionary> gives the name of a file containing a dictionary to be used" << endl;
        cout << "   (rather than have the Obfuscator generate its own replacements). If a" << endl;
        cout << "   required symbol is absent from the dictionary, however, the Obfuscator will" << endl;
        cout << "   generate additional dictionary entries. If omitted, the Obfuscator generates" << endl;
        cout << "   its own dictionary. One can see if the Obfuscator had to generate additional" << endl;
        cout << "   entries by specifying both -d and -u, and comparing the output dictionary" << endl;
        cout << "   (-d) with input (-u)." << endl;
        cout << "-c Indicates that literals should be obfuscated." << endl;
        cout << "-x Indicates that identifiers should be obfuscated." << endl;
        cout << "   At least one of -c or -x must be specified." << endl;
        cout << "-l Causes replacement symbols to be assigned in lexicographical order. If not" << endl;
        cout << "   specified, replacement symbols are assigned in encounter order." << endl;
        cout << "-h Causes this usage information to be displayed." << endl;
        cout << endl;
        rc = 1;
    }
    else
    {
        Obfuscator obfuscator(inputFile, outputFile, 
            inputDictionary, outputDictionary, 
            obfuscateConstants, obfuscateIdentifiers, encounterOrder);

        if (!obfuscator.run())
        {
            cout << "Obfuscation failed due to errors." << endl;
            rc = 2;
        }
    }

    return rc;
}
