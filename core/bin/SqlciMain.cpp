

#include "qmscommon/QRLogger.h"
#include "sqlci/SqlciEnv.h"

SqlciEnv *global_sqlci_env = nullptr;

int main(int argc, char *argv[]) {
  ios::sync_with_stdio();

  SqlciEnv *sqlci = new SqlciEnv();
  global_sqlci_env = sqlci;

  QRLogger::initLog4cplus(QRLogger::QRL_MXEXE);

  sqlci->run();

  delete sqlci;
}
