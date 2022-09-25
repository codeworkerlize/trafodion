#!/bin/bash

cgroups=$(ps -o cgroup --no-headers $$)
if [[ $cgroups == "-" ]]
then
  echo ""
  echo "#Resource static pools not enabled"
  echo "export ESGYN_CG_CPU=''" 
  echo "export ESGYN_CGP_CPU=''" 
  echo "export ESGYN_CG_CPUACCT=''" 
  echo "export ESGYN_CGP_CPUACCT=''" 
  echo "export ESGYN_CG_MEM=''" 
  echo "export ESGYN_CGP_MEM=''" 
else
  if [[ ! $(id -Gn) =~ trafodion ]]
  then
    echo "Error: Current user ($(id -un)) not member of trafodion group." >&2
    echo "   Group membership required to enable tenant cgroup permissions." >&2
    exit 1
  fi
  echo ""
  cut -d: -f2,3 --output-delimiter=' ' /proc/$$/cgroup | \
    while read cg cgpath
    do
      if [[ $cg =~ cpu$|cpu, ]]
      then
        echo "export ESGYN_CG_CPU=${cg}:$cgpath" 
        mnt=$(lssubsys -m cpu | cut -f2 -d' ')
        echo "export ESGYN_CGP_CPU=$mnt${cgpath}" 
      fi
      if [[ $cg =~ cpuacct ]]
      then
        echo "export ESGYN_CG_CPUACCT=${cg}:$cgpath" 
        mnt=$(lssubsys -m cpuacct | cut -f2 -d' ')
        echo "export ESGYN_CGP_CPUACCT=$mnt${cgpath}" 
      fi
      if [[ $cg =~ memory ]]
      then
        echo "export ESGYN_CG_MEM=${cg}:$cgpath" 
        mnt=$(lssubsys -m memory | cut -f2 -d' ')
        echo "export ESGYN_CGP_MEM=$mnt${cgpath}" 
      fi
    done
fi
exit 0
