
MAKEFLAGS += --no-print-directory

TARGTYPES = linux 
HELP_TARGETS = all help $(TARGTYPES:%=%help)
GLOBAL_TARGETS = clobber distclean  copy-orc-jars 
ORC_VER = 1.5.0
ORC_TOOLS_DIR = $(TOOLSDIR)/ORC-$(ORC_VER)-Linux/share
ORC_UBER_JAR_FILE = orc-tools-$(ORC_VER)-uber.jar
ORC_CORE_JAR_FILE = orc-core-$(ORC_VER).jar

# The simple way to do this work is to recursively call another make process
# with the proper TARGTYPE and FLAVOR.  However, GNU make isn't supported
# real well under Windows (primarily related to job support).  In order to
# provide a good build environment for SQL/MX, we try to run everything within
# the context of one make process.  This gets pretty tricky.
#
# This top-level makefile's main goal is to determine the correct TARGTYPE
# and FLAVOR before including Makerules.mk.

# Only support one goal.  For now, it would probably be too hard to ensure that
# we could build targets of two different TARGTYPEs correctly.  It probably
# isn't too important that we are able to do that.
ifneq (1,$(words $(MAKECMDGOALS)))
  ifneq (0,$(words $(MAKECMDGOALS)))
    $(error We dont currently support more than one target... Sorry)
  endif
endif

# Determine the target that we are building for.
TARGTYPE := $(strip $(foreach targ,$(TARGTYPES),$(if $(filter $(targ)%,\
    $(MAKECMDGOALS)),$(targ))))

# Try to find the TARGTYPE from a string like one of the following:
#   ../lib/nt/debug/tdm_arkcmp.exe
#   optimizer/nt/debug/OptAll.obj
ifndef TARGTYPE
TARGTYPE := $(strip $(foreach targ,$(TARGTYPES),$(if $(findstring /$(targ)/,\
    $(MAKECMDGOALS)),$(targ))))
endif

# Determine if this is a debug or release target.
ifneq (,$(findstring release,$(MAKECMDGOALS)))
  FLAVOR := release
else
  ifneq (,$(findstring debug,$(MAKECMDGOALS)))
    FLAVOR := debug
  else 
    ifneq (,$(findstring doc,$(MAKECMDGOALS)))
      FLAVOR := doc
    endif
  endif
endif

# Determine if the target is for security
ifneq (,$(findstring sec,$(MAKECMDGOALS)))
  SECURITY := true
else
  SECURITY := false
endif

# Set machine type for linux system, for other system, ARCHBITS is empty
ifeq ($(TARGTYPE),linux)
ifeq ($(SQ_MTYPE),64)
ARCHBITS := 64bit
else
ARCHBITS := 32bit
endif
endif

# Save a copy of the CLASSPATH of the environment.
ENV_CLASSPATH := $(CLASSPATH)

# This is needed to prevent a syntax problem when including the
# Makerules.$(target) below
define incMRtemplate
  include $(1)
endef

# Handle help targets
ifneq (,$(findstring $(MAKECMDGOALS),$(HELP_TARGETS)))
  ifdef TARGTYPE
    include Makerules.$(TARGTYPE)
  else
    $(foreach target,$(TARGTYPES),$(eval $(call incMRtemplate,Makerules.$(target))))
  endif
endif

ifdef MAKECMDGOALS
  # If a rule was given that is not handled directly by this file, then
  # give an error if either TARGTYPE or FLAVOR could not be determined.
  # Otherwise, everything should be OK for building so Makerules.mk is
  # included.
  ifeq (,$(filter $(MAKECMDGOALS),$(HELP_TARGETS) $(GLOBAL_TARGETS) ))
    ifndef TARGTYPE
      $(error Cannot determine the target type)
    endif
    include Makerules.mk
  endif
else
  .DEFAULT_GOAL=help
  $(foreach target,$(TARGTYPES),$(eval $(call incMRtemplate,Makerules.$(target))))
endif



help::
	@echo "$(MAKE) clobber              Removes all objects from all targets"

copy-orc-jars: $(TRAF_HOME)/export/lib/$(ORC_CORE_JAR_FILE) $(TRAF_HOME)/export/lib/$(ORC_UBER_JAR_FILE)

$(TRAF_HOME)/export/lib/$(ORC_CORE_JAR_FILE): $(ORC_TOOLS_DIR)/$(ORC_CORE_JAR_FILE)
	cp -Pf $< $@

$(TRAF_HOME)/export/lib/$(ORC_UBER_JAR_FILE): $(ORC_TOOLS_DIR)/$(ORC_UBER_JAR_FILE)
	cp -Pf $< $@

$(ORC_TOOLS_DIR)/$(ORC_CORE_JAR_FILE):
$(ORC_TOOLS_DIR)/$(ORC_UBER_JAR_FILE):
	$(TRAF_HOME)/tools/orc-build.sh

clobber distclean:
	@echo $@ is not yet implemented.
