

CPPSRC := arkcmp.cpp

EARLY_DLLS := -ljsig




CPPSRC += StoredProcInterface.cpp
CPPSRC += vproccmp.cpp

# Define the correct compilation and linking flags depending on whether
# this is a debug or release build.
ifeq ($(FLAVOR),debug)
   # for coverage checking support
   ifeq ($(SQ_COVERAGE_OPTIMIZER),1)
      OPT_EXE_LDFLAGS += --coverage
   endif
   ifeq ($(SQ_COVERAGE_REFRESH),1)
      OPT_EXE_LDFLAGS += --coverage
   endif
endif


TARGET := tdm_arkcmp
DEP_LIBS := \
	sqlcomp \
	cli \
	comexe \
	common \
	eh \
	sort \
	sqlmsg \
	tdm_sqlerrormsg \
	tdm_sqlexport \
	tdm_sqlshare \
	tdm_sqlmxevents \
	sqlcat \
	parser \
	optimizer \
	generator \
	optimizer \
	generator \
	arkcmplib \
	sqlcat \
	executor \
	tdm_sqlcli \
	tdm_sqlexp \
	porting_layer \
	lmcomp \
        smdio \
	qmscommon 


DEP_LIBS += sqlcomp

EXTERN_LIBS := \
	$(XMPIROOT)/libstmlib.so \
	$(XMPIROOT)/libsqauth.so \
	$(LIBROOT)/libtdm_measenty.so \
	$(LIBROOT)/libtdm_security.so \
	$(LIBROOT)/libtdm_tfds.so \
	$(LIBROOT)/libtdm_tmflib.so \
	$(LIBROOT)/libdummy.so \
	$(XMPIROOT)/libsbms.so \
	$(XMPIROOT)/libsbfs.so \
	$(LIBROOT)/libtdm_tmfuser.so \
	$(XMPIROOT)/libwin.so

SYS_LIBS := -lrt -lpthread 





INCLUDE_DIRS := sqlcmpdbg sqlco sqludr

SRCPATH := cli bin
