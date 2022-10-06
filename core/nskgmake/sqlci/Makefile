

CPPSRC := SqlciMain.cpp 

EARLY_DLLS:= -ljsig

EXTERN_LIBS := \
        $(XMPIROOT)/libstmlib.so \
	$(XMPIROOT)/libsqauth.so 

DEP_LIBS += tdm_sqlcli sqlcilib arkcmp_dll tdm_sqlexp tdm_sqlexport cli executor generator  sqlcomp tdm_sqllangman tdm_sqlerrormsg qmscommon arkcmplib parser optimizer comexe common

ifdef BUILD_ORC_READER
DEP_LIBS += orc
endif

CPPSRC += vprocsqlci.cpp


SRCPATH := sqlci bin

