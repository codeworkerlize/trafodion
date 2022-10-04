

#include "sqlci/SqlciEnv.h"
#include "qmscommon/QRLogger.h"

SqlciEnv *global_sqlci_env = nullptr;

int main(Int32 argc, char *argv[]) {
  ios::sync_with_stdio();

  SqlciEnv *sqlci = new SqlciEnv();
  global_sqlci_env = sqlci;

  QRLogger::initLog4cplus(QRLogger::QRL_MXEXE);

  sqlci->run();

  delete sqlci;
}
