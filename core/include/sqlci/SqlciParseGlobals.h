
#ifndef SQLCIPARSEGLOBALS_H
#define SQLCIPARSEGLOBALS_H

#undef GLOB_
#undef INIT_
#ifdef SQLCIPARSEGLOBALS__INITIALIZE
#define GLOB_
#define INIT_(val) = val
#else
#define GLOB_ extern
#define INIT_(val)
#endif

void SqlciLexReinit();
int sqlciparse();

class SqlciNode;  // Forward refs to keep #include dependencies minimal
class SqlciEnv;   // "

GLOB_ UInt32 SqlciParse_InputPos INIT_(0);
GLOB_ char *SqlciParse_InputStr INIT_(NULL);
GLOB_ char *SqlciParse_OriginalStr INIT_(NULL);
GLOB_ int SqlciParse_HelpCmd INIT_(0);
GLOB_ int SqlciParse_IdentifierExpected INIT_(0);
GLOB_ int SqlciParse_SyntaxErrorCleanup INIT_(0);

// this global variable returns the final parse tree
GLOB_ SqlciNode *SqlciParseTree INIT_(NULL);
GLOB_ SqlciEnv *SqlciEnvGlobal INIT_(NULL);

#undef GLOB_
#undef INIT_

#endif 
