
CPPSRC := memlogshow.cpp

ifeq ($(FLAVOR),debug)
     CXXFLAGS += -DNA_DEBUG_GUI -rdynamic -Wl,--export-dynamic
endif

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

TARGET := memlogshow

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

SRCPATH := tool

