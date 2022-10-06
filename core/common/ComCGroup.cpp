// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#include "common/ComCGroup.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "cli/Globals.h"
#include "common/Ipc.h"
#include "common/NAUserId.h"
#include "export/ComDiags.h"

ComCGroup::ComCGroup(NAMemory *heap) : heap_(heap), cgroupName_(heap), assignWasDone_(FALSE) {}

int ComCGroup::setOrAlterCGroup(const char *newCGroupName, ComDiagsArea &diags) {
  int result = 0;
  NAString prevCGroupName = cgroupName_;  // save it for error handling
  if (cgroupName_ != newCGroupName) {
    char pidBuf[20];
    const int numCGroups = 3;
    const char *cGroupDefs[] = {"ESGYN_CGP_MEM", "ESGYN_CGP_CPU", "ESGYN_CGP_CPUACCT"};
    NAString prevPath;
    int numRetries = 6;

    cgroupName_ = newCGroupName;
    snprintf(pidBuf, sizeof(pidBuf), "%d\n", (int)getpid());

    for (int cg = 0; cg < numCGroups && !result; cg++) {
      const char *cgp = getenv(cGroupDefs[cg]);

      if (!cgp) {
        result = recordAssignError(diags, cGroupDefs[cg], "getenv", 0);
        break;
      }

      NAString cgroupPath(cgp);

      if (cgroupPath != prevPath) {
        prevPath = cgroupPath;
        if (cgroupName_.length() > 0 && cgroupName_ != DB__SYSTEMTENANT) {
          cgroupPath.append("/");
          cgroupPath.append(cgroupName_);
        }
        cgroupPath.append("/cgroup.procs");

        int fd = open(cgroupPath.data(), O_WRONLY | O_APPEND);

        if (fd >= 0) {
          int len = strlen(pidBuf);
          int nb = write(fd, pidBuf, len);

          if (nb >= len)
            assignWasDone_ = TRUE;
          else
            result = recordAssignError(diags, cgroupPath.data(), "write", errno);

          if (close(fd) < 0) result = recordAssignError(diags, cgroupPath.data(), "close", errno);
        } else {
          // Couldn't open the cgroup.
          //
          // The most likely explanation is that the cgroup
          // does not yet exist and that we need to call a JNI
          // interface to create it. It could also be a permissions
          // issue.
          //
          // If we are trying to assign this process to the
          // default tenant and if this object wasn't assigned
          // to a tenant previously, then we tolerate this
          // error (we assume we are in an environment that
          // does not use tenants and may not have the
          // requisite cgroups set up).
          if (assignWasDone_ || (cgroupName_.length() > 0 && cgroupName_ != DB__SYSTEMTENANT)) {
            int savedErrno = errno;

            if (GetCliGlobals()->createLocalCGroup(cgroupName_.data(), diags) == 0) {
              // retry
              cg--;
              prevPath = "";

              if (--numRetries < 0) {
                result = recordAssignError(diags, cgroupPath.data(), "retries", savedErrno);
                break;
              }
            } else {
              // diags area is already set, but also record the open error
              result = recordAssignError(diags, cgroupPath.data(), "open", savedErrno);
              break;
            }
          }
        }
      }  // this cgroup is the first or uses a different path
    }    // for
  }      // cgroup name changed

  if (result < 0)  // revert to the original cgroup if there was an error
    cgroupName_ = prevCGroupName;
  return result;
}

NABoolean ComCGroup::isSystemTenant() const { return cgroupName_ == DB__SYSTEMTENANT; }

int ComCGroup::recordAssignError(ComDiagsArea &diags, const char *pathName, const char *assignPhase, int linuxError) {
  int result = -8748;  // the SQLCODE created
  IpcProcessId myProcId = GetCliGlobals()->getEnvironment()->getMyOwnProcessId(IPC_DOM_GUA_PHANDLE);
  char buf[200];

  diags << DgSqlCode(result);
  myProcId.addProcIdToDiagsArea(diags, 0);
  diags << DgString1(pathName);

  switch (linuxError) {
    case EACCES:
      snprintf(buf, sizeof(buf), "Permission denied (EACCES)");
      break;

    case ENAMETOOLONG:
      snprintf(buf, sizeof(buf), "File name too long (ENAMETOOLONG)");
      break;

    case ENOENT:
      snprintf(buf, sizeof(buf), "Non-existent file or cgroup (ENOENT)");
      break;

    default:
      if (strcmp(assignPhase, "getenv") == 0)
        snprintf(buf, sizeof(buf), "Environment variable %s is not set up", pathName);
      else if (strcmp(assignPhase, "retries") == 0)
        snprintf(buf, sizeof(buf), "Retries exhausted");
      else
        snprintf(buf, sizeof(buf), "Could not %s file, errno = %d", assignPhase, linuxError);
      break;
  }

  diags << DgString2(buf);

  return result;
}
