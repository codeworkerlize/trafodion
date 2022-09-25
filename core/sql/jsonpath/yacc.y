// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

%{
#include "Platform.h"
#include "NABoolean.h"
#include "NAStringDef.h"
#include "CmpCommon.h"
#include "ComDiags.h"

#include "JPInterface.h"
#include "jp_parser_def.h"
#define YY_LOG_FILE "jplog"

union YYSTYPE;

extern "C"{

    void yyerror(void* scaner, const char *s);
}

extern int yylex(YYSTYPE * lvalp, void* scaner);

extern __thread  CollHeap *JPHeap;
extern __thread  PathNode *theSearchPathTree;

#define scanner scaner

%}

%lex-param {void * scaner}
%parse-param {void * scaner}
%define api.pure full

%union
{
    NAString *m_string;
    Int32 m_int;
    char m_op;
    PathNode *m_node;
    BasicCompnt *m_expr;
    IndexNode *m_index;
    JPOperatorType m_type;
}


%start search_path

%token <m_int> TOK_INTEGER
%token <m_string> IDENTIFIER
%token <m_string> TOK_LAX
%token <m_string> TOK_STRICT
%token <m_string> TOK_TO
%token <m_string> TOK_TRUE
%token <m_string> TOK_FALSE
%token <m_string> TOK_NULL
%token <m_op> TOK_QUOTE
%token <m_op> '.'
%token <m_op> '['
%token <m_op> ']'
%token <m_op> '('
%token <m_op> ')'
%token <m_op> ','
%token <m_op> '{'
%token <m_op> '}'
%token <m_op> '$'
%token <m_op> '?'
%token <m_op> '*'
%token <m_op> '@'
%token <m_op> '>'

%left '>' '=' '<'
%left '+' '-'
%left '*' '/'

%type <m_int> empty
%type <m_node> search_path
%type <m_node> path
%type <m_node> head_node
%type <m_node> node
%type <m_node> node_list
%type <m_node> node_idx
%type <m_expr> filter_pattern
%type <m_expr> filter
%type <m_string> key
%type <m_string> key_pattern
%type <m_node> index_list
%type <m_index> index 
%type <m_index> index_expression 
%type <m_expr> condition
%type <m_op> start_filter
%type <m_string> boolean_factor
%type <m_expr> numeric_factor
%type <m_expr> numeric_term
%type <m_expr> numeric_value_expression
%type <m_expr> cmp_value_expression
%type <m_expr> boolen_term
%type <m_expr> comparison_operator
%type <m_type> if_greater_eq
%type <m_type> if_less_eq
%type <m_expr> if_equal_operator
%type <m_expr> logic_operator
%type <m_expr> string_term
%type <m_expr> comparison_predicate
%type <m_expr> logic_predicate

%%

search_path  : TOK_LAX  path
    {
        yyerror(scanner, "Lax mode is not supported yet.");
        YYERROR;

        $2->setJsonMode(PathNode::LAX_MODE);
        theSearchPathTree = $2;
    }
    | TOK_STRICT path
    {
        $2->setJsonMode(PathNode::STRICT_MODE);
        theSearchPathTree = $2;
    }
    | path
    {
        $1->setJsonMode(PathNode::STRICT_MODE);
        theSearchPathTree = $1;
    }

path : head_node 
    {
        $$ = $1;
    }
    |  head_node node_list
    {
        if ($2 != NULL)
        {
            PathNode *last = $1->getChildLast();
            last->addToTree($2);
        }

        $$ = $1;
    }

head_node : '$' filter_pattern
    {
        NAString *buf = new(JPHeap) NAString("$", JPHeap);

        $$ = new(JPHeap) PathNode(PathNode::PathNodeType_Key, buf);
        if (NULL != $2)
            $$->setFilterExpr($2);
    }
    | '$' index_list filter_pattern
    {
        NAString *buf = new(JPHeap) NAString("$", JPHeap);

        PathNode *head = new(JPHeap) PathNode(PathNode::PathNodeType_Key, buf);
        head->addToTree($2);

        if ($3 != NULL)
        {
            PathNode *last = head->getChildLast();
            last->setFilterExpr($3);
        }

        $$ = head;
    }

node_list : node
    {
        $$ = $1;
    }
    |  node node_list
    {
        $1->getChildLast()->addToTree($2);
        $$ = $1;
    }

node : '.' key_pattern filter_pattern
    {
        NAString *buf = new(JPHeap) NAString(*$2);
        PathNode *node = new(JPHeap) PathNode(PathNode::PathNodeType_Key, buf);

        if (0 == buf->compareTo("*"))
            node->setNodeKeyWildcard(TRUE);
        if (NULL != $3)
            node->setFilterExpr($3);

        $$ = node;
    }
    | '.' key_pattern index_list filter_pattern
    {
        NAString *buf = new(JPHeap) NAString(*$2);
        PathNode *node = new(JPHeap) PathNode(PathNode::PathNodeType_Key, buf);

        if (0 == buf->compareTo("*"))
            node->setNodeKeyWildcard(TRUE);

        node->addToTree($3);
        if ($4 != NULL)
        {
            node->getChildLast()->setFilterExpr($4);
        }

        $$ = node;
    }

index_list : node_idx
    {
        $$ = $1;
    }
    |  node_idx index_list
    {
        $1->addToTree($2);
        $$ = $1;
    }

node_idx : '[' index ']'
    {
        NAString *buf = new(JPHeap) NAString("*");
        PathNode *node = new(JPHeap) PathNode(PathNode::PathNodeType_Idx, buf, $2);

        $$ = node;
    }

filter_pattern : empty
    {
      $$ = NULL;
    }
    |  filter
    {
      $$ = $1;
    }

filter : start_filter '(' condition ')'
    {
        $$ = $3;
    }

start_filter : '?'

condition : logic_predicate
    {
        $$ = $1;
    }
    | comparison_predicate
    {
        $$ = $1;
    }

logic_predicate : comparison_predicate logic_operator comparison_predicate
    {
        $2->setChild($1, $3);
        $$ = $2;    
    }
    | logic_predicate logic_operator comparison_predicate
    {
        $2->setChild($1, $3);
        $$ = $2;
    }

comparison_predicate : cmp_value_expression comparison_operator cmp_value_expression
    {
        $2->setChild($1, $3);
        $$ = $2;
    }
    | boolen_term
    {
        $$ = $1;    
    }

cmp_value_expression: numeric_value_expression
    {
        $$ = $1;
    }
    | string_term
    {
        $$ = $1;    
    }

logic_operator : '&' '&'
    {
        $$ = new(JPHeap) BiLogicOp(AND_OP);
    }
    | '|' '|'
    {
        $$ = new(JPHeap) BiLogicOp(OR_OP);
    }

boolen_term : cmp_value_expression if_equal_operator boolean_factor
    {
        JPOperatorType tmp;
            
        if ($3->compareTo("true") == 0)
            tmp = IS_TRUE;
        else if ($3->compareTo("false") == 0)
            tmp = IS_FALSE;
        else if ($3->compareTo("NULL") == 0)
            tmp = IS_NULL;
        BoolCmpnt *bool_tmp = new(JPHeap) BoolCmpnt(tmp);
        $2->setChild($1, bool_tmp);
        $$ = $2;
    }

comparison_operator : if_greater_eq
    {
        $$ = new(JPHeap) BiRelOp($1);
    }
    | if_less_eq
    {
        $$ = new(JPHeap) BiRelOp($1);
    }
    | if_equal_operator
    {
        $$ = $1;
    }

if_greater_eq : '>'
    {
        $$ = GREATER;
    }
    | if_greater_eq '='
    {
        $$ = GREATER_EQ;
    }

if_less_eq : '<'
    {
        $$ = JPLESS;
    }
    | if_less_eq '='
    {
        $$ = LESS_EQ;       
    }

if_equal_operator : '=' '='
    {
        $$ = new(JPHeap) BiRelOp(EQUAL);
    }
    | '!' '='
    {
         $$ = new(JPHeap) BiRelOp(NOT_EQUAL);
    }

numeric_value_expression : numeric_term
    {
        $$ = $1;
    }
    | numeric_value_expression '+' numeric_term
    {
        $$ = new(JPHeap) BiArithOp(PLUS, $1, $3); 
    } 
    | numeric_value_expression '-' numeric_term
    {
        $$ = new(JPHeap) BiArithOp(MINUS, $1, $3);
    }

numeric_term : numeric_factor
    {
        $$ = $1;
    }
    | numeric_term '*' numeric_factor
    {
        $$ = new(JPHeap) BiArithOp(MULTIPLY, $1, $3); 
    }
    | numeric_term '/' numeric_factor
    {
        $$ = new(JPHeap) BiArithOp(DIVIDE, $1, $3);
    }

numeric_factor : TOK_INTEGER
    {
        $$ = new(JPHeap) NumericCmpnt($1);
    }
    | '@'
    {
        $$ = new(JPHeap) NumericCmpnt(0);
        ((NumericCmpnt*)$$)->setIsAt(true);
    }   

string_term : '"' IDENTIFIER '"'
    {
        NAString *buf = new(JPHeap) NAString(JPHeap);
        *buf = $2->data();
        $$ = new(JPHeap) StringCmpnt(buf);
    }

boolean_factor : TOK_TRUE
    | TOK_FALSE
    | TOK_NULL


index : index_expression
    {
        $$ = $1;
    }       
     | index ',' index_expression
    {
        $1->setNext($3);
        $$ = $1;
    }

index_expression : TOK_INTEGER
    {
        $$  = new(JPHeap) IndexNode($1);
    }
    | '*'
    {
        $$ = new(JPHeap) IndexNode(0);
        $$->setIsStar(true);
    }
    | TOK_INTEGER TOK_TO TOK_INTEGER
    {
        $$ = new(JPHeap) IndexNode($1, $3);
    }

key_pattern : '*'
    {
        $$ = new(JPHeap) NAString("*");
    }
    | key
    {
        $$ = $1;
    }
      
key : IDENTIFIER
    {
        $$ = $1;
    }   

empty :     { $$ = 0; }

%%

