#pragma once

class SqlciNode;
class SqlciEnv;

int sqlci_parser(char *instr, char *origstr, SqlciNode **node, SqlciEnv *sqlci_env);
int sqlci_parser_syntax_error_cleanup(char *instr, SqlciEnv *sqlci_env);
