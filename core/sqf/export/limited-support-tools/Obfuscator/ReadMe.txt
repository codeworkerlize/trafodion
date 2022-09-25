This directory contains the source code for a SQL Obfuscator program.

This program can be used to obfuscate identifiers and/or literals in
SQL statements. This can be useful in support situations where a
customer does not wish to disclose identifiers or literal data.

To build this program, simply do a "make" in this directory.

To get help, simply run the program with the "-h" command line 
parameter. At the time this file was written, the help text is:

Usage: ./Obfuscator.exe -i <input file> [-o <output file>] [-d <dictionary file>] [-u <dictionary file>] [-c] [-x] [-l]

This program, the Obfuscator, is used to obfuscate identifiers or literals or
both in SQL text.

-i <input file> gives the name of the file containing SQL text to be
   obfuscated. This is a required parameter.
-o <output file> gives the name of a file where obfuscated text should be
   written. If omitted, no obfuscated text is written.
-d <dictionary> gives the name of a file where the set of substitutions to be
   used by the Obfuscator should be written. This is useful if the user wishes
   to tailor the substitutions before actually making them. It is also useful
   if one wishes to keep a record of the substitutions made. If omitted, this
   file is not written.
-u <dictionary> gives the name of a file containing a dictionary to be used
   (rather than have the Obfuscator generate its own replacements). If a
   required symbol is absent from the dictionary, however, the Obfuscator will
   generate additional dictionary entries. If omitted, the Obfuscator generates
   its own dictionary. One can see if the Obfuscator had to generate additional
   entries by specifying both -d and -u, and comparing the output dictionary
   (-d) with input (-u).
-c Indicates that literals should be obfuscated.
-x Indicates that identifiers should be obfuscated.
   At least one of -c or -x must be specified.
-l Causes replacement symbols to be assigned in lexicographical order. If not
   specified, replacement symbols are assigned in encounter order.
-h Causes this usage information to be displayed.

