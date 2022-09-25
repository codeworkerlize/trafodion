
SHELL := /bin/bash

java_files:=$(shell find src/main/java -name "*.java*")

DEBUG		=

all:  $(PROGS) mavenbuild

define javabuild
	@mkdir -p src/main/resources
	$(TRAF_HOME)/export/include/SCMBuildJava.sh 1.0.1 >src/main/resources/esgyn-snmp.jar.mf
	set -o pipefail; $(MAVEN) -f $(1) package -DskipTests | tee maven_build.log | grep -e '\[INFO\] Building' -e '\[INFO\] BUILD SUCCESS' -e 'ERROR'
endef

$(TRAF_HOME)/export/lib/jdmkrt-1.0.jar: $(TOOLSDIR)/open-jdmk-1.0/jdmkrt-1.0.jar
	cp -p $(TOOLSDIR)/open-jdmk-1.0/jdmkrt-1.0.jar $(TRAF_HOME)/export/lib

target/esgyn-snmp-${TRAFODION_VER}.jar: $(java_files) $(TRAF_HOME)/export/lib/jdmkrt-1.0.jar
	$(call javabuild, pom.xml)

mavenbuild: target/esgyn-snmp-${TRAFODION_VER}.jar
	cp -pf target/esgyn-ping-*.jar $(TRAF_HOME)/export/lib

setup:
	@echo "OUTDIR =" $(OUTDIR)
	@echo "JAVA_HOME =" $(JAVA_HOME)
	@# do nothing

depcheck:
	$(MAVEN) org.owasp:dependency-check-maven:check

mvn_clean:
	-$(MAVEN) -f pom.xml clean | grep -e '\[INFO\] Building' -e '\[INFO\] BUILD SUCCESS' -e 'ERROR'

clean: $(shell if [ -d target ]; then echo "mvn_clean"; fi) 
	$(RM) $(PROGS)
	$(RM) -rf target
	$(RM) -rf $(TRAF_HOME)/export/lib/esgyn-ping-*.jar

cleanall: clean
	$(RM) -rf $(BUILD_PLAT)

cleaner: clean
	$(RM) *~

