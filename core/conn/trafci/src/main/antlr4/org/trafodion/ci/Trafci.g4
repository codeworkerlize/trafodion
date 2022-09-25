grammar Trafci;

@header {
    package org.trafodion.ci.parser;
}

program : create_routine_stmt
        | declare_stmt
        | begin_stmt
        | other_stmt
        ;

create_routine_stmt : T_CREATE (T_OR T_REPLACE)? (T_TRIGGER | T_PROCEDURE | T_FUNCTION | T_PACKAGE T_BODY?) .*? (T_AS | T_IS) .*?
                    ;

declare_stmt : T_DECLARE ~T_CURSOR .*?
             ;

begin_stmt : T_BEGIN ~(T_WORK | T_TRANSACTION | T_SAVEPOINT) .*?
           ;

other_stmt : .*?
           ;

// Lexer rules
T_AS              : A S ;
T_BEGIN           : B E G I N ;
T_BODY            : B O D Y ;
T_CREATE          : C R E A T E ;
T_CURSOR          : C U R S O R ;
T_DECLARE         : D E C L A R E ;
T_FUNCTION        : F U N C T I O N ;
T_IS              : I S ;
T_OR              : O R ;
T_PACKAGE         : P A C K A G E ;
T_PROCEDURE       : P R O C E D U R E ;
T_REPLACE         : R E P L A C E ;
T_SAVEPOINT       : S A V E P O I N T ;
T_TRANSACTION     : T R A N S A C T I O N ;
T_TRIGGER         : T R I G G E R ;
T_WORK            : W O R K ;


L_ID        : L_ID_PART                                                // Identifier
            ;
L_S_STRING  : '\'' (('\'' '\'') | ('\\' '\'') | ~('\''))* '\''         // Single quoted string literal
            ;
L_D_STRING  : '"' (L_STR_ESC_D | .)*? '"'                              // Double quoted string literal
            ;

L_WS        : L_BLANK+ -> channel(HIDDEN) ;                            // Whitespace
L_M_COMMENT : '/*' .*? '*/' -> channel(HIDDEN) ;                       // Multiline comment
L_S_COMMENT : ('--') .*? '\r'? '\n' -> channel(HIDDEN) ; 

fragment
L_ID_PART  :
             [a-zA-Z] ([a-zA-Z] | L_DIGIT | '_')*                      // Identifier part
            | '$' '{' .*? '}'
            | ('_' | '@' | ':' | '#' | '$') ([a-zA-Z] | L_DIGIT | '_' | '@' | ':' | '#' | '$')+     // (at least one char must follow special char)
            | '"' .*? '"'                                                   // Quoted identifiers
            | '[' .*? ']'
            | '`' .*? '`'
            ;
fragment
L_STR_ESC_D :                                                          // Double quoted string escape sequence
              '""' | '\\"' 
            ;            
fragment
L_DIGIT     : [0-9]                                                    // Digit
            ;
fragment
L_BLANK     : (' ' | '\t' | '\r' | '\n')
            ;

// Support case-insensitive keywords and allowing case-sensitive identifiers
fragment A : ('a'|'A') ;
fragment B : ('b'|'B') ;
fragment C : ('c'|'C') ;
fragment D : ('d'|'D') ;
fragment E : ('e'|'E') ;
fragment F : ('f'|'F') ;
fragment G : ('g'|'G') ;
fragment H : ('h'|'H') ;
fragment I : ('i'|'I') ;
fragment J : ('j'|'J') ;
fragment K : ('k'|'K') ;
fragment L : ('l'|'L') ;
fragment M : ('m'|'M') ;
fragment N : ('n'|'N') ;
fragment O : ('o'|'O') ;
fragment P : ('p'|'P') ;
fragment Q : ('q'|'Q') ;
fragment R : ('r'|'R') ;
fragment S : ('s'|'S') ;
fragment T : ('t'|'T') ;
fragment U : ('u'|'U') ;
fragment V : ('v'|'V') ;
fragment W : ('w'|'W') ;
fragment X : ('x'|'X') ;
fragment Y : ('y'|'Y') ;
fragment Z : ('z'|'Z') ;
