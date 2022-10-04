

CPPSRC := Define.cpp \
	ExpConvMxcs.cpp \
	Formatter.cpp \
	InputStmt.cpp \
	Log.cpp \
	Obey.cpp \
	Param.cpp \
	Prepare.cpp \
	ShellCmd.cpp \
	SqlciCmd.cpp \
	SqlciEnv.cpp \
	SqlciError.cpp \
	SqlciNode.cpp \
	SqlciParser.cpp \
	SqlciReset.cpp \
	SqlciShow.cpp \
	SqlciStats.cpp \
	SqlciStmts.cpp \
	SqlCmd.cpp \
	sqlclicmd.cpp


YINC := tdm_sqlmxmsg_intl

DEFS += -DSQLCLI_LIB -DCLI_DLL -D_UNICODE -DCOPYRIGHT_VERSION_H=\"$(TRAFODION_VER)\" -DCOPYRIGHT_HEADER_H='"$(PRODUCT_COPYRIGHT_HEADER)"' -DPRODVER='"$(TRAFODION_VER_PROD)"'

CPPSRC += vers_libsqlcilib.cpp

YSRC := sqlci_yacc.y
LSRC := sqlci_lex.ll
YACC_VAR_PREFIX := sqlci
LEX_PREFIX := sqlci

SRCPATH := sqlci exp

# /home/esoye/trafodion/core/sql/nskgmake/sqlcilib/linux/64bit/debug

$(TARGOBJDIR)/$(LEX_PREFIX)/sqlci_lex.$(OBJSUFFIX) : $(TARGOBJDIR)/sqlci_yacc.h
