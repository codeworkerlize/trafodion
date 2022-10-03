// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
#ifndef COMCGROUP_H
#define COMCGROUP_H

#include "common/NAString.h"

class ComDiagsArea;

// ------------------------------------------------------------------
// Methods dealing with cgroups at the OS level, interfacing with
// libcgroup (this assumes the libcgroup-devel RPM is installed)
//
// Mostly, this is just a simple interface layer. However, we will
// be starting a large number of processes, and our metadata just
// contains a simple cgroup name, not the path. Therefore, we add
// a "cgroup string" as an internal representation of the path or
// paths where the cgroup or cgroups are mounted. This string can
// be sent to other processes, so we don't have to find the mount
// points for every ESP or similar process we start. This string
// is only for internal use of the methods declared here, it is
// not meant to be interpreted by callers.
// ------------------------------------------------------------------

class ComCGroup
{
public:

  // create an empty object
  ComCGroup(NAMemory *heap);

  // change the cgroup in this object and assign this
  // process to the new cgroup
  int setOrAlterCGroup(const char *newCGroupName,
                       ComDiagsArea &diags);

  NABoolean isSystemTenant() const;

private:

  // record error information in the diags area when we fail
  // to assign a process to a cgroup
  int recordAssignError(ComDiagsArea &diags,
                        const char *pathName,
                        const char *assignPhase,
                        int linuxError);

  NAMemory *heap_;
  NAString cgroupName_;
  NABoolean assignWasDone_;
};

#endif
