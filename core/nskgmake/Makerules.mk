
SHELL := /bin/bash
ifeq ($(filter-out linux, $(TARGTYPE)),$(TARGTYPE))
$(error Makerules.mk is included improperly. TARGTYPE is incorrectly set)
endif

ifeq ($(filter-out release debug doc, $(FLAVOR)),$(FLAVOR))
$(error Makerules.mk: Flavor isn't set correctly in target: $(MAKECMDGOALS))
endif

# Set some of the variables needed by this Makefile.
TOPDIR := ..
TOPLIBDIR := lib
TOPDLLDIR := dll
RESULTDIR := $(TOPDIR)/$(TOPLIBDIR)/$(TARGTYPE)/$(ARCHBITS)/$(FLAVOR)
DLLRESULTDIR := $(TOPDIR)/$(TOPDLLDIR)
LOGFILE := $(TARGTYPE)$(FLAVOR).log


BUILD_TARGET=1

ifneq (,$(findstring help, $(MAKECMDGOALS)))
 BUILD_TARGET=0
endif

# Prepare output directory for final objects and output log
# file when we aren't doing a "make help".
ifeq ($(BUILD_TARGET),1)
  $(info Loading component makefiles ...)

# Create output directory for final objects if it doesn't exist.
  _dummy := $(if $(wildcard $(RESULTDIR)),,$(shell mkdir -p $(RESULTDIR)))

endif

SHELL := sh

YACC       = export BISON_PKGDATADIR=$(TOPDIR)/toolbin/bison; export M4=$(TOPDIR)/toolbin/m4; $(TOPDIR)/toolbin/bison.exe -p $(YACC_VAR_PREFIX)
LEX        = $(TOPDIR)/toolbin/flex.exe -P$(YACC_VAR_PREFIX)
AWK       := awk.exe

# Build everything by default
.DEFAULT_GOAL := buildall

# Don't allow old-style implicit rules.
.SUFFIXES:
.SUFFIXES: .h .cpp .obj .lib .dll .exe .tlo .o

# BASE_INCLUDE_DIRS defines the directories that are included during compilation.
BASE_INCLUDE_DIRS := sqlci arkcmp comexe sqlfe eh export sqlmsg sqlcomp \
	sqlcat executor parser generator exp filesystem optimizer cli \
	nskcre common dml arkfsindp2 arkfsinopen ddl sort catman \
	smdio  sqlshare sqlmxevents bin langman sqludr udrserv \
	security runtimestats qmscommon qms porting_layer  parquet \
	tool

# These rules display the messages on the console as SQL/MX compiles.
ifndef VERBOSE
COMPILE_ECHO_RULE = @echo "Compiling $<";
LINK_TLO_ECHO_RULE = @echo "Linking library $@";
LINK_LIB_ECHO_RULE = @echo "Linking library $@";
LINK_LIB_DLL_ECHO_RULE = @echo "Creating export file and DLL .lib file $@";
LINK_DLL_ECHO_RULE = @echo "Linking DLL library $@";
BUILD_RC_ECHO_RULE = @echo "Building resource file $@";
LINK_EXE_ECHO_RULE = @echo "Linking executable $@";
LEX_ECHO_RULE = @echo "Generating C++ code from lex file $<";
YACC_ECHO_RULE = @echo "Generating C++ code from yacc file $<";
GENERATE_ECHO_RULE = @echo "Generating file $@";
endif

define starting_logfile
	OUTFILE=tmp_$(@F)_$$$$.txt; \
	printf '%s\n' "### Starting: $$HEADING" > $$OUTFILE;
endef

define capture_output
	OUTFILE=tmp_$(@F)_$$$$.txt; \
	echo "===============================================================" > $$OUTFILE; \
	printf '%s\n' "$$HEADING" >> $$OUTFILE; \
	echo "===============================================================" >> $$OUTFILE; \
	printf '%s\n' "$$CMD" >> $$OUTFILE; \
	if [[ -z "$(DRYRUN)" ]]; then \
	  eval $$CMD >> $$OUTFILE 2>&1; \
	  CMD_RETURN=$$?; \
	else \
	  CMD_RETURN=0; \
	fi; \
	if [[ -n "$(CMDS_DIR)" ]]; then \
	  printf '%s\n' "$$CMD" > $(CMDS_DIR)/$(subst _$(TARGTYPE)_,-,$(subst _$(FLAVOR)_,_,$(subst /,_,$(subst ../,,$(@))))); \
	fi; \
        if [ $$CMD_RETURN != 0 ]; then cat $$OUTFILE; fi; \
	rm -f $$OUTFILE; \
        exit $$CMD_RETURN;
endef

# Include the platform-specific rules.
include Makerules.$(TARGTYPE)

# The following values prefixed with "DUMMY_" define what the names of
# the libraries are likely to be.  This allows the proper makefiles
# to be included.  However, the makefiles may rename the libraries or
# executables, so the real final names are appended to names prepended
# with "FINAL_".
DUMMY_LIBS := $(patsubst %,%.$(LIBSUFFIX),$(LIB_DIRS))
ifdef DLLSUFFIX
DUMMY_DLLS := $(patsubst %,%.$(DLLSUFFIX),$(DLL_DIRS))
else
DUMMY_DLLS := $(DLL_DIRS)
endif
ifdef EXESUFFIX
DUMMY_EXES := $(patsubst %,%.$(EXESUFFIX),$(EXE_DIRS))
else
DUMMY_EXES := $(EXE_DIRS)
endif

# These values prefixed with "FINAL_" are built up within Makerules.build
# as each target is included.  Each new target is appended to the
# appropriate variable.  These lines below aren't really necessary, but
# are here to make this Makefile easier to understand.
FINAL_LIBS :=
FINAL_DLLS :=
FINAL_EXES :=
FINAL_INSTALL_OBJS :=

# These rules are used as part of a mechanism to compile the files
# located in different source locations.  This template is called from
# the Makerules.build file.  It is used for compiling C++ code.  It
# makes a call to "build_cpp_rule", which is platform-specific.
CPP_OBJ = $(TARGOBJDIR)/$(basename $(notdir $(1))).$(OBJSUFFIX)
DEP_FILE = $(TARGOBJDIR)/depend/d_$(basename $(notdir $(1))).dep
define CPP_BUILD_template
$(CPP_OBJ): $(1)
	$$(build_cpp_rule)

$(CPP_OBJ) : DEP_FILE:=$(DEP_FILE)
$(CPP_OBJ) : CPP_OBJ:=$(CPP_OBJ)
endef

CC_OBJ = $(TARGOBJDIR)/$(basename $(notdir $(1))).$(OBJSUFFIX)
define CC_BUILD_template
$(CC_OBJ): $(1)
	$$(build_cpp_rule)

$(CC_OBJ) : DEP_FILE:=$(DEP_FILE)
$(CC_OBJ) : CC_OBJ:=$(CC_OBJ)
endef

# These rules are used as part of a mechanism to compile the files
# located in different source locations.  This template is called from
# the Makerules.build file.  It is used for compiling C code.  It
# makes a call to "build_c_rule", which is platform-specific.
C_OBJ = $(TARGOBJDIR)/$(basename $(notdir $(1))).$(OBJSUFFIX)
define C_BUILD_template
$(C_OBJ): $(1)
	$$(build_c_rule)

$(C_OBJ) : DEP_FILE:=$(DEP_FILE)
$(C_OBJ) : C_OBJ:=$(C_OBJ)
$(C_OBJ) : C_INC_OVERRIDE:=$(C_INC_OVERRIDE)
endef



compile_c_resultobj_rule = $(CXX) $(DEBUG_FLAGS) $(SQLCLIOPT) $(ALL_INCLUDES) -o $@ -c $<

build_c_resultobj_rule = $(COMPILE_ECHO_RULE) \
		HEADING="Compiling $(<) --> $(@)"; $(starting_logfile) \
		CMD="$(compile_c_resultobj_rule)"; $(capture_output)

# This rule template builds an object in the RESULTDIR directory.
C_RESULTOBJ = $(RESULTDIR)/$(INSTALL_OBJ)
define C_RESULTOBJ_template
$(C_RESULTOBJ): $(1)
	$$(build_c_resultobj_rule)

$(C_RESULTOBJ) : SQLCLIOPT:=$(SQLCLIOPT)
$(C_RESULTOBJ) : ALL_INCLUDES:=$(ALL_INCLUDES)
$(C_RESULTOBJ) : DEP_FILE:=$(DEP_FILE)
$(C_RESULTOBJ) : CPP_OBJ:=$(CPP_OBJ)
endef

# BISON_SIMPLE defines which bison.simple file to use.
YACC_PREFIX=
BISON_SIMPLE=$(TOPDIR)/toolbin/bison.simple

# This creates the rules for creating the C++ code from the YACC files
# and for compiling the code.  This template is used within Makerules.build.
# This may be a little more complex than it needs to be, but it seems to
# work for most cases.  Removing the .cpp file from TARGOBJDIR without
# removing the .h file does cause problems though.
define YACC_BUILD_template
$(1).h: $(2)
	$$(YACC_ECHO_RULE) $$(build_yacc_rule)

$(1).cpp: $(1).h $(2)

$(1).$(OBJSUFFIX): $(1).cpp $(1).h
	$$(build_cpp_rule)

$(1).$(OBJSUFFIX): ALL_INCLUDES:=$(ALL_INCLUDES)
$(1).$(OBJSUFFIX): ALL_DEFS:=$(ALL_DEFS)
$(1).$(OBJSUFFIX): BISON_SIMPLE:=$(BISON_SIMPLE)
$(1).$(OBJSUFFIX): DEP_FILE:=$(DEP_FILE)
# .SECONDARY: $(1).cpp
endef

# base_lex_rule defines how to generate c++ code from lex files in
# all directories.
base_lex_rule = $(LEX) -iB -o$(basename $@).cpp $<;\
	$(AWK) -f ./flexstep.awk arkstr="$(LEX_PREFIX)" \
	   $(basename $@).cpp > $(basename $@).cpp.tmp;

# This is how the C++ code is generated from lex code.
build_lex_rule = rm -f $(basename $@).cpp;\
	$(base_lex_rule)\
	cp -fpv $(basename $@).cpp.tmp $(basename $@).cpp

# This rule template defines the dependencies and rules for creating
# the C++ code from a lex file and for compiling the C++ code.
define LEX_BUILD_template
$(1).$(OBJSUFFIX): ALL_INCLUDES:=$(ALL_INCLUDES)
$(1).$(OBJSUFFIX): ALL_DEFS:=$(ALL_DEFS)
$(1).$(OBJSUFFIX): LEX_PREFIX:=$(LEX_PREFIX)
$(1).$(OBJSUFFIX): DEP_FILE:=$(DEP_FILE)
.SECONDARY: $(1).cpp

$(1).cpp: $(2)
	$$(LEX_ECHO_RULE) $$(build_lex_rule)

$(1).$(OBJSUFFIX): $(1).cpp
	$$(build_cpp_rule)
endef

# This section of the Makefile loops through all of the targets and
# sets "obj" to the name of the target before including Makerules.build.
# Then Makerules.build uses the "obj" value to determine which of the
# individual makefiles should be included.  We only include all of the
# lower makefiles when we aren't doing a "make help".
ifeq ($(BUILD_TARGET),1)
define include_template
  obj := $(1)
  include Makerules.build
endef
ALL_DUMMY_TARGETS := $(DUMMY_LIBS) $(DUMMY_DLLS) $(DUMMY_EXES)
$(foreach target,$(ALL_DUMMY_TARGETS),$(eval $(call include_template,$(target))))
endif

.PHONY: $(MAKECMDGOALS)




../src/main/resources/trafodion-sql.jar.mf:
	# create a jar manifest file with the correct version information
	mkdir -p ../src/main/resources
	$(TRAF_HOME)/export/include/SCMBuildJava.sh 1.0.1 > ../src/main/resources/trafodion-sql.jar.mf

build_jar_manifest: | ../src/main/resources/trafodion-sql.jar.mf




buildall: $(FINAL_LIBS) $(FINAL_DLLS) $(FINAL_INSTALL_OBJS) $(FINAL_EXES)    

clean:
	@echo "Removing intermediate objects for $(TARGTYPE)/$(ARCHBITS)/$(FLAVOR)"
	@rm -rf */$(TARGTYPE)/$(ARCHBITS)/$(FLAVOR)
	@rmdir */$(TARGTYPE) > /dev/null 2>&1 || true
	@echo "Removing final objects directory"
	@rm -rf $(RESULTDIR)
	@rmdir $(TOPDIR)/$(TOPLIBDIR)/$(TARGTYPE) > /dev/null 2>&1 || true
	@echo "Removing $(LOGFILE) and $(LOGFILE).old"
	@rm -rf $(LOGFILE) $(LOGFILE).old
	@echo "Removing coverage files"
	@-find $(TOPDIR) -maxdepth 1 -name '*.gcov' -print | xargs rm -f
	@cd ..; $(MAVEN) clean
	@rm -rf $(TRAF_HOME)/export/lib/trafodion-sql-*.jar
	@rm -rf ../src/main/resources/trafodion-sql.jar.mf
	@rm -rf $(TRAF_HOME)/export/lib/binlogproducer-*.jar

