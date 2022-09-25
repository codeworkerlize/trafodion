#!/bin/bash
#
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#
# make sure the environment can build the code of trafodion or not.
# must configure the yum repo right before execute this script.
# run this script with normal user, while must has sudo permission.

function usage {

cat <<EOF

Syntax: $0 [ -y ] [ -h | -help | --help | -v | -version | --version ]
Usage:

    1. You can create building environment with this script 
    2. You must have sudo privileges before running this script.
    3. This script will change the following environment:
        a) Checking and installing basic commands(like as yum lsb_release awk cut uname).
        b) Checking the os version.
        c) Installing all libraries which is depended by EsgynDB.
        d) Removing pdsh and qt-dev if had installed.
        e) Checking and setting the java environment.
        f) Changing $HOME/.bashrc(Add JAVA_HOME, LANG, LC_LANG and TOOLSDIR, modify PATH)
        g) Calling the script traf_tools_setup.sh to install the depended tools.
    4. Getting more information from the link:
        https://cwiki.apache.org/confluence/display/TRAFODION/Create+Build+Environment
    5. It's best to download all the necessary softwares and set the environment variable "MY_SW_HOME" before running this script.
    6. You can set optional environment variables as following:
        a) MY_JVM_PATH: set the specific JVM path
        b) MY_JAVA_VER: set the specific JDK version name
        c) MY_SUDO: set the specific sudo command
        d) MY_INSTALLER: set the command for installing packages 
        e) MY_LOCAL_SW_DIST:
    7. This script maybe create the directory "${MY_DOWNLOAD_SW_DIST}" if not exits.

EOF
}

#default path
MY_JVM_PATH=${MY_JVM_PATH-"/usr/lib/jvm"}
MY_JAVA_VER=${MY_JAVA_VER-"java-1.8.0-openjdk"}
MY_SUDO=${MY_SUDO-"sudo"}
MY_INSTALLER=${MY_INSTALLER-"${MY_SUDO} yum"}

# for setup tools, 
MY_SW_HOME=${MY_SW_HOME-"/add_your_local_shared_folder_here"}
MY_LOCAL_SW_DIST=${MY_LOCAL_SW_DIST-${MY_SW_HOME}/local_software_tools}
MY_INSTALL_SW_DIST=${MY_INSTALL_SW_DIST-${MY_SW_HOME}/installed_software_tools}
MY_DOWNLOAD_SW_DIST=${MY_DOWNLOAD_SW_DIST-${MY_SW_HOME}/download_software_tools}

MY_IMPLICIT_Y="n"
while [ $# -gt 0 ];
do
  case $1 in
      -y) MY_IMPLICIT_Y="y"
	  ;;
      -h|-help|--help)
	  usage
	  exit 0
	  ;;
      -v|-version|--version)
	  echo "version 0.0.0.1"
	  exit 0
	  ;;
      *)  echo "ERROR: Unexpected argument $1"
	  usage
	  exit 1
	  ;;
  esac
  shift
done


LOGFILE=${LOGFILE-$(pwd)/$0.log}

# check the permission.
(${MY_SUDO} touch /etc/trafpermise) >>${LOGFILE}2>&1
if [ "x$?" != "x0" ]; then
    echo "ERROR: you must run this script with sudo without password permission."
    exit 1
fi

OS=`uname -o`
ARCH=`uname -m`
echo "INFO: os is [${OS}], cpu architecture is [${ARCH}]"

# check some basic commands
yumpath=`which yum`
if [ "x$?" != "x0" ]; then
    aptpath=`which apt`
    if [ "x$?" != "x0" ]; then
	echo "ERROR: You must first install yum or apt."
	exit 1
    else
        MY_INSTALLER="sudo apt-get"
    fi
fi
echo "INFO: use installer [${MY_INSTALLER}]"

LLVM_VERSION=${LLVM_VERSION-"3.2"}
PROTOBUF_VERSION=${PROTOBUF_VERSION-"2.5.0"}
basecmds=(lsb_release awk cut)
if [ "x${OS}" = "xGNU/Linux" ]; then
    case ${ARCH} in
        aarch*)
	    LLVM_VERSION=3.8.1
	    PROTOBUF_VERSION=2.6.1
	    ;;
        x86_64)
	    ;;
        *)
	    echo "ERROR: The platform [${ARCH}] is not supported!"
	    exit 1
    esac
else
    echo "ERROR: The OS [${OS}] is not supported!"
    exit 1
fi

for basecmd in ${basecmds[@]}  
do
    basecmdpath=`which ${basecmd}`
    if [ "x$?" != "x0" ]; then
	case ${basecmd} in
	    lsb_release)
		(${MY_INSTALLER} -y install redhat-lsb) >>${LOGFILE}2>&1
		if [ "x$?" = "x0" ]; then
		    echo "ERROR: yum repo server has an error when running command [${MY_INSTALLER} install redhat-lsb]."
		    exit 1
		fi
		;;
	    *)
		echo "ERROR: command [${basecmd}] does not exist. Make sure you have installed it, and have added it to the command path."
		exit 1
	esac
    fi
    echo "INFO: command ${basecmd} exists"
done

# check the local software directory
local_sws=(udis llvm mpich bison icu zookeeper thrift apache-maven protobuf apache-log4cxx hadoop)
http_sws=(
http://sourceforge.net/projects/udis86/files/udis86/1.7/udis86-1.7.2.tar.gz
http://llvm.org/releases/${LLVM_VERSION}/llvm-${LLVM_VERSION}.src.tar.gz
http://www.mpich.org/static/downloads/3.0.4/mpich-3.0.4.tar.gz
http://ftp.gnu.org/gnu/bison/bison-3.0.tar.gz
http://download.icu-project.org/files/icu4c/4.4/icu4c-4_4-src.tgz
https://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz
http://archive.apache.org/dist/thrift/0.9.0/thrift-0.9.0.tar.gz
http://archive.apache.org/dist/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz
https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz
https://dist.apache.org/repos/dist/release/logging/log4cxx/0.10.0/apache-log4cxx-0.10.0.tar.gz
http://archive.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz
)

if [ ! -d ${MY_LOCAL_SW_DIST} ]; then
    echo "WARNING: Local software tools aren't present. Will download all tools from the internet. This will be very slow. If you do have the local software tools present, set the environment variable MY_LOCAL_SW_DIST to point to them and run this script again. The default local software directory is [${MY_LOCAL_SW_DIST}]. Do you want to continue? Enter y/n (default: n):"
    if [ "x$MY_IMPLICIT_Y" = "xn" ]; then
	read YN
	case ${YN} in
	    Y|y|Yes|YES)
		;;
	    *)
		echo "Downloading the following build tools from the internet:"
		for i in `seq ${#local_sws[@]}`
		do
		    printf "%2d.%15s: %s\n" ${i} ${local_sws[${i}-1]} ${http_sws[${i}-1]}
		done
		exit 1
		;;
	esac
    fi
else
    # check the local software's source exist or not
    for local_sw in ${local_sws[@]}
    do
	local_file=`ls ${MY_LOCAL_SW_DIST} | grep ${local_sw}`
	if [ "x${local_file}" = "x" ]; then
            echo "WARNING: [${local_sw}] source file does not exist in directory [${MY_LOCAL_SW_DIST}]"
	fi
    done
fi

osdistributor=`lsb_release -is`
osver=`lsb_release -rs`
case ${osver} in
    *n/a*)
	osver="7.0"
	;;
    *)
esac

osvermajor=`echo $osver | cut -d . -f 1-1`
osverminor=`echo $osver | cut -d . -f 2-2`
case ${osdistributor} in
    *RedHat*|*CentOS*)
	${MY_INSTALLER} -y install epel-release >>${LOGFILE}2>&1
	;;
    *NeoKylin*)
	${MY_INSTALLER} -y install neokylin-lsb-core >>${LOGFILE}2>&1
	;;
    *EulerOS*)
        ${MY_INSTALLER} -y install redhat-lsb-core >>${LOGFILE}2>&1
	${MY_INSTALLER} -y install nodejs >>${LOGFILE}2>&1
        ;;
    Kylin*|kylin*)
	${MY_INSTALLER} -y install neokylin-lsb-core >>${LOGFILE}2>&1
	${MY_INSTALLER} -y install nodejs >>${LOGFILE}2>&1
	;;
    *openEuler*)
	${MY_INSTALLER} -y install nodejs >>${LOGFILE}2>&1
	;;
    *)
	echo "WARN: OS [${osdistributor}] don't support epel-release, use another way to install packages!"
	;;
esac

# install all libs
baselibs=(
           alsa-lib-devel ant apr-devel                                \
           apr-util-devel boost-devel cmake device-mapper-multipath    \
           dhcp doxygen flex gcc-c++ gd git glibc-devel                \
           graphviz-perl gzip ${MY_JAVA_VER}-devel                     \
           libX11-devel libXau-devel libaio-devel libcurl-devel        \
           libibumad-devel libiodbc automake cppzmq-devel libuuid-devel\
           libiodbc-devel librdmacm-devel                              \
           libxml2-devel lua-devel lzo-minilzo net-snmp-devel          \
           net-snmp-perl openldap-clients openldap-devel openmotif     \
           openssl-devel perl-Config-IniFiles perl-Config-Tiny         \
           perl-DBD-SQLite perl-Expect perl-IO-Tty perl-Math-Calc-Units \
           perl-Params-Validate perl-Parse-RecDescent perl-TermReadKey \
           perl-Time-HiRes protobuf-compiler protobuf-devel            \
           readline-devel rpm-build saslwrapper sqlite-devel unixODBC  \
           unixODBC-devel uuid-perl wget xerces-c-devel xinetd         \
           ncurses-devel snappy snappy-devel lzo lzo-devel libcgroup   \
           libcgroup-devel byacc)


echo "INFO: os distributor id is [${osdistributor}]"
case ${osdistributor} in
    *RedHat*|*CentOS*)
        case ${osvermajor} in
            6)
	        echo "INFO: os version is [$osver]"
		speciallibs=(ant-nodeps libibumad)
	        ;;
            7)
                echo "INFO: os version is [$osver]"
		speciallibs=(libibumad librdmacm)
                ;;
            *)
	        echo "ERROR: Linux OS version is [$osver]. Only 6.x and 7.x versions are presently supported on RedHat and CentOS."
	        exit 1
        esac
	;;
    *openEuler*)
	echo "INFO: os version is [$osver]"
baselibs=(
           alsa-lib-devel apr-devel                                \
           apr-util-devel boost-devel cmake device-mapper-multipath    \
           dhcp doxygen flex gcc-c++ gd git glibc-devel                \
           graphviz-perl gzip ${MY_JAVA_VER}-devel                     \
           libX11-devel libXau-devel libaio-devel libcurl-devel        \
           libibumad-devel automake libuuid-devel\
           librdmacm-devel libibumad librdmacm          \
           libxml2-devel lua-devel lzo-minilzo net-snmp-devel          \
           net-snmp-perl openldap-clients openldap-devel openmotif     \
           openssl-devel perl-Config-IniFiles         \
           perl-DBD-SQLite \
           perl-TermReadKey \
           perl-Time-HiRes protobuf-compiler protobuf-devel            \
           readline-devel rpm-build sqlite-devel unixODBC  \
           unixODBC-devel uuid-perl wget xinetd         \
           ncurses-devel snappy snappy-devel lzo lzo-devel libcgroup   \
           libcgroup-devel byacc)
          # speciallibs=(ant libiodbc cppzmq-devel libiodbc-devel perl-Config-Tiny perl-Expect perl-IO-Tty perl-Math-Calc-Units perl-Params-Validate perl-Parse-RecDescent saslwrapper xerces-c-devel)
	;;
    *EulerOS*)
        case ${osvermajor} in
	    2)
		echo "INFO: os version is [$osver]"
		baselibs=(
		    alsa-lib-devel ant apr-devel                                \
 		    apr-util-devel boost-devel cmake device-mapper-multipath    \
		    doxygen flex gcc-c++ gd git glibc-devel                \
		    gzip ${MY_JAVA_VER}-devel                     \
		    libX11-devel libXau-devel libaio-devel libcurl-devel        \
		    libibumad-devel automake libuuid-devel\
		    librdmacm-devel librdmacm libibumad          \
		    libxml2-devel lzo-minilzo net-snmp-devel          \
		    openldap-clients openldap-devel openmotif     \
		    openssl-devel perl-Config-IniFiles         \
		    perl-DBD-SQLite \
		    perl-Params-Validate perl-TermReadKey \
		    perl-Time-HiRes protobuf-devel            \
		    readline-devel rpm-build sqlite-devel unixODBC  \
		    unixODBC-devel wget xinetd         \
		    ncurses-devel snappy snappy-devel lzo lzo-devel libcgroup   \
		    libcgroup-devel byacc)

		#speciallibs=(dhcp protobuf-compiler graphviz-perl cppzmq-devel libiodbc libiodbc-devel lua-devel net-snmp-perl perl-Config-Tiny perl-Expect perl-IO-Tty perl-Math-Calc-Units perl-Parse-RecDescent saslwrapper uuid-perl xerces-c-devel)
		speciallibs=()
	        ;;
	    *)
	        echo "ERROR: EulerOS OS version is [$osver]. Only 2.x versions are presently supported on EulerOS."
	        exit 1
        esac
	;;

    Kylin*|kylin*)
        case ${osvermajor} in
            4)
	        echo "INFO: os version is [$osver]"
		baselibs=(automake libssl-dev byacc bison flex libevent-dev ant     \
			  libboost-dev liblog4cxx10-dev libncurses-dev ldap-utils   \
			  libreadline-dev libcurl4-openssl-dev libsqlite-dev slapd  \
			  libsqlite3-dev libcgroup-dev libxml2-dev git zip gawk     \
			  libaprutil1 libsnappy-dev libprotobuf-dev libdbi-perl rpm \
			  sqlite libdbd-sqlite3-perl openjdk-8-jdk liblzo2-dev curl \
			  libcurl4-openssl-dev keepalived unixodbc-dev cgroup-tools \
			  sqlite3)
		speciallibs=()
	        ;;
	    7)
	       
                echo "INFO: os version is [$osver]"
		baselibs=(alsa-lib-devel ant apr-devel                                \
			  apr-util-devel boost-devel cmake device-mapper-multipath    \
			  dhcp doxygen flex gcc-c++ git glibc-devel                   \
			  gzip ${MY_JAVA_VER}-devel                                   \
			  libX11-devel libXau-devel libaio-devel libcurl-devel        \
			  libibumad-devel libiodbc automake libuuid-devel             \
			  libiodbc-devel librdmacm-devel libxml2-devel lzo-minilzo    \
			  openldap-devel openssl-devel perl-DBD-SQLite                \
			  perl-TermReadKey           \
			  readline-devel sqlite-devel unixODBC unixODBC-devel wget    \
			  ncurses-devel snappy snappy-devel lzo lzo-devel libcgroup   \
			  libcgroup-devel byacc)
		speciallibs=(libibumad librdmacm)
		;;
	    10)
	       
                echo "INFO: os version is [$osver]"
		baselibs=(alsa-lib-devel ant apr-devel                                \
			  apr-util-devel boost-devel cmake device-mapper-multipath    \
			  dhcp doxygen flex gcc-c++ gd git glibc-devel                \
			  graphviz-perl gzip ${MY_JAVA_VER}-devel                     \
			  libX11-devel libXau-devel libaio-devel libcurl-devel        \
			  libibumad-devel libiodbc automake libuuid-devel             \
			  libiodbc-devel librdmacm-devel                              \
			  libxml2-devel lua-devel lzo-minilzo net-snmp-devel          \
			  net-snmp-perl openldap-clients openldap-devel openmotif     \
			  openssl-devel perl-Config-IniFiles          \
			  perl-DBD-SQLite perl-Expect perl-IO-Tty  \
			  perl-TermReadKey \
			  protobuf-compiler protobuf-devel            \
			  readline-devel rpm-build sqlite-devel unixODBC  \
			  unixODBC-devel uuid-perl wget xerces-c-devel xinetd         \
			  ncurses-devel snappy snappy-devel lzo lzo-devel libcgroup   \
			  libcgroup-devel byacc)

		speciallibs=(protobuf libibumad librdmacm)
		;;
            *)
	        echo "ERROR: Kylin OS version is [$osver]. Only 4.x versions are presently supported."
	        exit 1
        esac
	;;

    NeoKylin*)
	echo "INFO: os version is [$osver]"
	baselibs=(
           alsa-lib-devel ant apr-devel                                \
           apr-util-devel boost-devel cmake device-mapper-multipath    \
           dhcp doxygen flex gcc-c++ gd git glibc-devel                \
           gzip ${MY_JAVA_VER}-devel                     \
           libX11-devel libXau-devel libaio-devel libcurl-devel        \
           libibumad-devel libiodbc automake libuuid-devel\
           librdmacm-devel                              \
           libxml2-devel lzo-minilzo          \
           openldap-clients openldap-devel openmotif     \
           openssl-devel         \
           perl-DBD-SQLite \
           perl-Params-Validate perl-TermReadKey \
           perl-Time-HiRes            \
           readline-devel rpm-build sqlite-devel unixODBC  \
           unixODBC-devel wget xinetd         \
           ncurses-devel snappy lzo libcgroup   \
           byacc)
	;;

    *)
	echo "ERROR: The OS distribution is [${osdistributor}], but Trafodion only supports RedHat and CentOS presently."
	exit 1
esac

(${MY_INSTALLER} -y install npm) >>${LOGFILE}2>&1
(${MY_SUDO} npm install -g bower) >>${LOGFILE}2>&1

# install all libs
dependlibs=("${baselibs[@]}" "${speciallibs[@]}")
faillibs=()
echo "INFO: install dependent library start"
for dependlib in ${dependlibs[@]}
do
    (${MY_INSTALLER} -y install ${dependlib}) >>${LOGFILE}2>&1
    if [ "x$?" != "x0" ]; then
	faillibs=("${faillibs[@]}" "${dependlib}")
	echo "ERROR: install dependent library [${dependlib}] fail"
    else
	echo "INFO: install dependent library [${dependlib}] success"
    fi
done
echo "INFO: install dependent library finish"

# remove pdsh and qt-dev
echo "INFO: remove pdsh and qt-dev commands to avoid problems with trafodion scripts"
(${MY_INSTALLER} erase pdsh) >>${LOGFILE}2>&1
(${MY_INSTALLER} erase qt-dev) >>${LOGFILE}2>&1

# check and set the java
echo "INFO: check and set java environment"
javadir=`\ls -L ${MY_JVM_PATH} | grep "${MY_JAVA_VER}-" | head -n 1`
javapath="${MY_JVM_PATH}/${MY_JAVA_VER}"

if [ ! -d ${javapath} ]; then
     if [ -d ${MY_JVM_PATH}/${javadir} ]; then
	 echo "WARN: java dir [${javapath}] isn't exist! Try to link to exits path [${MY_JVM_PATH}/${javadir}]"
	 ${MY_SUDO} ln -sf ${MY_JVM_PATH}/${javadir} ${javapath}
     else
	 echo "ERROR: all of java dir [${javapath}] and [${MY_JVM_PATH}/${javadir}] aren't exist!"
	 exit 1
     fi
fi

. $HOME/.bashrc
javahome=`grep JAVA_HOME ~/.bashrc | wc -l`
if [ "x${javahome}" = "x0" ]; then
    echo -en "\n# Added by traf_checkset_env.sh of trafodion\n" >> $HOME/.bashrc
    echo -en "export JAVA_HOME=${javapath}\n" >> $HOME/.bashrc
    echo -en "export PATH=\$PATH:\$JAVA_HOME/bin\n" >> $HOME/.bashrc
    export JAVA_HOME=${javapath}
    export PATH=$PATH:$JAVA_HOME/bin
else
    java_version=`${JAVA_HOME}/bin/java -version 2>&1 | awk 'NR==1{ gsub(/"/,""); print $3}'`
    case ${java_version} in
	1.8.*)
	    echo "INFO: java version is [$java_version]"
	    ;;
	*)
	    echo "ERROR: java version is [$java_version]. Only 1.8.x versions are presently supported."
	    exit 1
  esac
fi

# check error
if (("${#faillibs[@]}" > "0")); then
    echo "ERROR: libs [${faillibs[@]}] aren't found in your repo server."
    exit 1;
fi

echo $'\n'"Build environment is created SUCCESS!"

echo $'\n'"Install the required Build tools start!"
export MY_LOCAL_SW_DIST
if [ ! -e ${MY_DOWNLOAD_SW_DIST} ]; then
    echo "INFO: mkdir [${MY_DOWNLOAD_SW_DIST}]"
    mkdir ${MY_DOWNLOAD_SW_DIST}
fi
echo "INFO: install tools with command:"\
"    [./traf_tools_setup.sh -d ${MY_DOWNLOAD_SW_DIST} -i ${MY_INSTALL_SW_DIST}]"
./traf_tools_setup.sh -d ${MY_DOWNLOAD_SW_DIST} -i ${MY_INSTALL_SW_DIST}
if [ "x$?" = "x0" ]; then
    echo $'\n'"Install the required Build tools SUCCESS!"
else
    echo "ERROR: Install the required Build tools FAIL.
    the execute command:
     [./traf_tools_setup.sh -d ${MY_DOWNLOAD_SW_DIST} -i ${MY_INSTALL_SW_DIST}]"
fi

tooldirexist=`grep TOOLSDIR ~/.bashrc | wc -l`
if [ "x${tooldirexist}" = "x0" ]; then
    echo -en "\n# Added by traf_checkset_env.sh of trafodion\n" >> $HOME/.bashrc
    echo -en "export TOOLSDIR=${MY_INSTALL_SW_DIST}\n" >> $HOME/.bashrc
    echo -en "export PATH=\$PATH:\$TOOLSDIR/apache-maven-3.3.3/bin\n" >> $HOME/.bashrc
    echo -en "export MY_LOCAL_SW_DIST=$MY_SW_HOME/local_software_tools\n" >> $HOME/.bashrc
fi

