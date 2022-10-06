

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "seabed/ms.h"
#include "seabed/fs.h"

#define FALSE 0

void
my_mpi_close()
{
  static short bMpiCloseCalled = 0;

  if (bMpiCloseCalled == 1) {
    return;
  }
  bMpiCloseCalled = 1;
#ifdef MPI_
  /*  msg_mon_close_process(msg_get_phandle("$SYSTEM"));
      msg_mon_close_process(msg_get_phandle("$DATA")); */
  msg_mon_process_shutdown();
#endif
}

void
my_mpi_fclose()
{
#ifdef MPI_
  /*  msg_mon_close_process(msg_get_phandle("$SYSTEM"));
      msg_mon_close_process(msg_get_phandle("$DATA"));*/
  file_mon_process_shutdown();
#endif
}

short my_mpi_setup (int* argc, char** argv[] )
{
  static short bMpiSetupCalled = 0;
  short retcode = 0;

  if (bMpiSetupCalled == 1) {
    return 0;
  }

  bMpiSetupCalled = 1;
#ifdef MPI_
  file_init_attach(argc,argv,1,"");
  retcode = file_mon_process_startup2(true, // sys messages? 
                                      false,// pipe stdout to the monitor
                                      false // remap stderr to the monitor (default: true)
                                      );
#endif

  if (retcode == 0) {
    msg_debug_hook("NGG", "ngg.hook");
  }

  return retcode;
}

