

CPPSRC := ex_esp_main.cpp \
	memorymonitor.cpp \
	vprocesp.cpp

TARGET := tdm_arkesp

EXTERN_LIBS := \
        $(XMPIROOT)/libstmlib.so \
        $(XMPIROOT)/libsqauth.so 

DEP_LIBS := tdm_sqlcli \
		porting_layer \
		cli	\
		comexe	\
		executor	\
		sqlmsg	\
		tdm_sqlexp \
		tdm_sqlexport \
		tdm_sqlerrormsg \
		common \
		sort \
		tdm_sqlmxevents \
		lmcomp \
		tdm_sqlshare \
		arkcmp_dll
ifeq ($(DIST_TYPE),Ubuntu)
DEP_LIBS += sqlcomp
endif


# ifneq (,$(PROFILE))

SRCPATH := bin cli catman smdio export runtimestats porting_layer
