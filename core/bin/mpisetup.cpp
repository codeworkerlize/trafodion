

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "common/Platform.h"
#include "seabed/ms.h"
#include "seabed/fs.h"

#define FALSE 0

void my_mpi_close() {
  static short bMpiCloseCalled = 0;

  if (bMpiCloseCalled == 1) {
    return;
  }
  bMpiCloseCalled = 1;
#ifdef MPI_
  msg_mon_process_shutdown();
#endif
}

void my_mpi_fclose() {
  static int sv_called_count = 0;
  static int sv_retcode = -999;
#ifdef MPI_
  if (++sv_called_count > 2000000000)  // Don't allow it to overflow to zero, but
    sv_called_count = 2000000000;      // keep the fact that it is very large.
  if (sv_called_count == 1) sv_retcode = file_mon_process_shutdown();
#endif
}

short my_mpi_setup(int *argc, char **argv[]) {
  static short bMpiSetupCalled = 0;
  short retcode = 0;

  if (bMpiSetupCalled == 1) {
    return 0;
  }

  bMpiSetupCalled = 1;
#ifdef MPI_
  file_init_attach(argc, argv, 1, (char *)"");
  retcode = file_mon_process_startup2(true,   // sys messages?
                                      false,  // pipe stdout to the monitor
                                      false   // remap stderr to the monitor (default: true)
  );
#endif

  if (retcode == 0) {
    msg_debug_hook("NGG", "ngg.hook");
    atexit(my_mpi_fclose);
  }

  return retcode;
}
