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

Summary:	Ambari management pack for EsgynDB
Name:		%{name}
Version:	%{version}
Release:	%{release}
AutoReqProv:	no
License:	TBD
Group:		Applications/Databases
Source0:        ambari_rpm.tar.gz
BuildArch:	noarch
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}
URL:            http://www.esgyn.com

Requires: ambari-server >= 2.4.2.0
Obsoletes: traf_ambari

Prefix: /opt/esgyn

%description
This extension enables management of EsgynDB via Ambari.


%prep
%setup -b 0 -n %{name} -c

%build

%install
cd %{_builddir}
mkdir -p %{buildroot}/opt/esgyn
cp -rf %{name}/* %{buildroot}/opt/esgyn

%post
$RPM_INSTALL_PREFIX/mpack-install/am_install.sh "$RPM_INSTALL_PREFIX" "traf-mpack" "%{version}-%{release}" "$1"

%postun
if [[ $1 == 0 ]]  # removing final version
then
  # no ambari command for this yet -- coming in Ambari 2.5.0
  rm -r /var/lib/ambari-server/resources/mpacks/QianBase-mpack*
  rm -r /var/lib/ambari-server/resources/common-services/QIANBASE
  rm -r /var/lib/ambari-server/resources/stacks/HDP/*/services/QIANBASE
  # clean up items created by am_install.sh script
  rm /opt/esgyn/traf-mpack*.tar.gz
fi

%clean
/bin/rm -rf %{buildroot}

%files
/opt/esgyn/upgrade_QianBase.py
/opt/esgyn/traf-mpack/
/opt/esgyn/mpack-install/
%exclude /opt/esgyn/*.pyc
%exclude /opt/esgyn/*.pyo

%changelog
* Fri Oct 21 2016 Steve Varnau
- ver 1.0
