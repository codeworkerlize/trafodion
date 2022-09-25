%{
#include "Platform.h"
#include "NABoolean.h"
#include "NAStringDef.h"
#include "CmpCommon.h"
#include "ComDiags.h"

#include "DgBaseType.h"
#include "ParserMsg.h"
#include "JPInterface.h"
#include "yacc.h"
#include "jp_parser_def.h"

#define yyerror jperror
#define YYPARSE_PARAM yyscan_t scaner
#define YYLEX_PARAM scaner

extern __thread  CollHeap *JPHeap;
extern __thread  char *jp_input;

extern ComDiagsArea *getDiag();

static int jp_cursor = 0;

#undef YY_INPUT
#define YY_INPUT(buffer,result,maxsize)\
{\
	buffer[0] = jp_input[jp_cursor++];\
	if ( !buffer[0] )\
		result = YY_NULL;\
	else\
		result=1;\
}
/*void scan_string(const char* str);*/
extern "C"
{
	int yywrap(void *)
	{
		return 1;
	}

	int yylex(void*);

    void yyerror(const char *s)
	{
		ComDiagsArea *diagPtr = getDiag();
		*diagPtr << DgSqlCode(-8983) << DgString0(s);
		StoreSyntaxError(jp_input, jp_cursor, *diagPtr);
	}

}

void resetLexer(yyscan_t scaner)
{
	jp_cursor = 0;
	jprestart(0, scaner);
}


%}

%option reentrant
%option bison-bridge

nondigit		([_A-Za-z])
digit			([0-9])
integer			({digit}+)
lax_mode		(lax)
strict_mode		(strict)
keyword_to		(to)
keyword_true	(true)
keyword_false	(false)
keyword_null	(null)
identifier		({nondigit}({nondigit}|{digit})*)
blank_chars		([\f\r\t\v]+)
left_paths		([\(])
right_paths		([\)])

	
%%
	
{lax_mode}		{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_LAX;
	}

{strict_mode}	{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_STRICT;
	}

{keyword_to}	{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_TO;	
	}

{keyword_true}	{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_TRUE;	
	}

{keyword_false}	{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_FALSE;	
	}

{keyword_null}	{
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return TOK_NULL;	
	}

{identifier}    {
	yylval->m_string = new(JPHeap) NAString(JPHeap);
	*yylval->m_string=yytext;
	return IDENTIFIER;
	}

{integer}        {
	yylval->m_int=atoi(yytext);
	return TOK_INTEGER;
	}

{blank_chars}    {
				 
	}

[\.]	{
	yylval->m_op=yytext[0];
    return '.';
	}

{left_paths}	{
	yylval->m_op=yytext[0];
	return '(';
	}

{right_paths}	{
	yylval->m_op=yytext[0];
	return ')';
	}

[\{]			{
	yylval->m_op=yytext[0];
	return '{';
	}

[\}]			{
	yylval->m_op=yytext[0];
	return '}';
	}

[\$]			{
	yylval->m_op=yytext[0];
	return '$';
	}

[\?]			{
	yylval->m_op=yytext[0];
	return '?';
	}

[\+]			{
	yylval->m_op=yytext[0];
	return '+';
	}

[\-]			{
	yylval->m_op=yytext[0];
	return '-';
	}

[\*]			{
	yylval->m_op=yytext[0];
	return '*';
	}

[\/]			{
	yylval->m_op=yytext[0];
	return '/';
	}

[\']			{
	yylval->m_op=yytext[0];
	return TOK_QUOTE;
	}

[\@]			{
	yylval->m_op=yytext[0];
	return '@';
	}

[\,]			{
	yylval->m_op=yytext[0];
	return ',';
	}

[\>]			{
	yylval->m_op=yytext[0];
	return '>';
	}

[\<]			{
	yylval->m_op=yytext[0];
	return '<';
	}

[\[]			{
	yylval->m_op=yytext[0];
	return '[';
	}

[\]]			{
	yylval->m_op=yytext[0];
	return ']';
	}

[\&]			{
	yylval->m_op=yytext[0];
	return '&';
	}

[\!]			{
	yylval->m_op=yytext[0];
	return '!';
	}

[\|]			{
	yylval->m_op=yytext[0];
	return '|';
	}

[\=]			{
	yylval->m_op=yytext[0];
	return '=';
	}

[\"]			{
	yylval->m_op=yytext[0];
	return '"';
	}

%%

void init_scaner(void* &scaner)
{
	yylex_init(&scaner);
}

void destroy_scaner(void* &scaner)
{
	yylex_destroy(scaner);
}
