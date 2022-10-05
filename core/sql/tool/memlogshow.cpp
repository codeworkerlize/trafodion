// **********************************************************************

// **********************************************************************

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

#include "ComMemLog.h"

void help() {
  printf("Usage: memlogshow pid\n");
  return;
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    help();
    exit(0);
  }

  long pid = atol(argv[1]);
  if (pid == 0) {
    help();
    exit(0);
  }

  int status;
  status = ComMemLog::instance().initialize(pid, true);
  if (status != 0) {
    printf("init failed: %s\n", strerror(status));
    return -1;
  }
  ComMemLog::instance().memshow();

  return 0;
}
