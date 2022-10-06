// **********************************************************************

// **********************************************************************

#include <ComCextdecs.h>
#include <long.h>

#include "QRQueriesImpl.h"
#include "QmpPublish.h"
#include "QmsMain.h"
#include "QueryRewriteServer.h"
#include "common/NAType.h"
#include "nsk/nskport.h"
#include "qmscommon/QRDescriptor.h"
#include "qmscommon/QRLogger.h"
#include "qmscommon/QRSharedPtr.h"
#include "seabed/fs.h"
#include "seabed/ms.h"
extern void my_mpi_fclose();
#include "common/SCMVersHelp.h"
DEFINE_DOVERS(tdm_arkqmp)

/**
 * \file
 * Contains the main() function for the Query Publisher Process, QMP, executable.
 * QMP is started by the Query Matching Monitor, QMM, process.
 * QMP reads MVQR-related defaults from the SYSTEM DEFAULTS table and
 * sends them to the QMM process.
 * QMP also reads from the MANAGEABILITY.MV_REWRITE.REWRITE_PUBLISH table.
 * Rows are read and PUBLISH requests are sent to the QMM process.
 * The QMM process then sends them to the Query Matching Server, QMS, processes in its queue.
 */

using namespace QR;

// The default is to publish to QMM.
PublishTarget publishTarget = PUBLISH_TO_QMM;

// Output file when PUBLISH_TO_FILE.
const char *targetFilename = NULL;

// This is needed to avoid a link error.
NABoolean NAType::isComparable(const NAType &other, ItemExpr *parentOp, int emitErr) const { return FALSE; }

void usage(char *progName) { cerr << "Usage: " << progName << " -target {QMM | QMS | FILE <filename>}" << endl; }

/**
 * No known input command-line arguments at this time
 *
 * @param argc Number of arguments on the command line.
 * @param argv None at this time.
 * @return TRUE if command line parsed correctly, FALSE if error.
 */
static NABoolean processCommandLine(int argc, char *argv[]) {
  // If no command line arguments are provided, the default is to publish to QMM.
  // -oss is unconditionally added by IpcGuardianServer::spawnProcess() when qmm
  // uses it to create qmp.

  if (argc == 1 || argc == 2 && !stricmp(argv[1], "-oss")) return TRUE;

  if (argc < 3 || stricmp(argv[1], "-target")) {
    usage(argv[0]);
    return FALSE;
  }

  if (!stricmp(argv[2], "QMM"))
    publishTarget = PUBLISH_TO_QMM;
  else if (!stricmp(argv[2], "QMS"))
    publishTarget = PUBLISH_TO_QMS;
  else if (!stricmp(argv[2], "FILE")) {
    if (argc < 4) {
      usage(argv[0]);
      return FALSE;
    }

    publishTarget = PUBLISH_TO_FILE;
    targetFilename = argv[3];
  }

  return TRUE;
}  // End of processCommandLine

extern "C" {
int sq_fs_dllmain();
}

int main(int argc, char *argv[]) {
  dovers(argc, argv);

  try {
    file_init_attach(&argc, &argv, TRUE, (char *)"");
    sq_fs_dllmain();
    msg_debug_hook("tdm_arkqmp", "tdm_arkqmp.hook");
    file_mon_process_startup(true);
    atexit(my_mpi_fclose);
  } catch (...) {
    cerr << "Error while initializing messaging system. Exiting..." << endl;
    exit(1);
  }

  NAHeap qmpHeap("QMP Heap", NAMemory::DERIVED_FROM_SYS_HEAP, (int)131072);

  QmpPublish qmpPublisher(&qmpHeap);

  // Establish the IPC heap and cache the IpcEnvironment ptr from
  // MvQueryRewriteServer.
  MvQueryRewriteServer::setHeap(&qmpHeap);
  qmpPublisher.setIpcEnvironment(MvQueryRewriteServer::getIpcEnv());

  int result = 0;

  QRLogger::instance().setModule(QRLogger::QRL_QMP);
  QRLogger::instance().initLog4cxx("log4cxx.qmp.config");

  QRLogger::log(CAT_QMP, LL_INFO, "MXQMP process was started.");

  char dateBuffer[30];  // Standard format is: YYYY-MM-DD HH:MM:SS.MMM.MMM
  MvQueryRewriteServer::getFormattedTimestamp(dateBuffer);
  // logMessage("\n\n\n================================================================================");
  QRLogger::log(CAT_QMP, LL_INFO, "Command-line QMP invoked with %d arguments.", argc);
  // for (int i=0; i<argc; i++)
  //  logMessage2("Program argument %d is %s", i, argv[i]);

#ifdef NA_WINNT
  if (getenv("QMP_MSGBOX_PROCESS") != NULL) {
    MessageBox(NULL, "QMP Process Launched", (CHAR *)argv[0], MB_OK | MB_ICONINFORMATION);
  };
#endif

  // Process any command-line arguments.
  if (processCommandLine(argc, argv) == FALSE) {
    QRLogger::log(CAT_QMP, LL_ERROR, "QMP processing of command-line arguments failed.");
    return -1;
  }

  if (qmpPublisher.setTarget(publishTarget, targetFilename) == FALSE) {
    QRLogger::log(CAT_QMP, LL_ERROR, "QMP opening publish target failed.");
    return -1;
  }

  // Process the REWRITE_PUBLISH table reading from the stream
  do {
    try {
      // result =
      qmpPublisher.performRewritePublishReading();
      QRLogger::log(CAT_QMP, LL_DEBUG, "QMP REWRITE_TABLE reading completed with result code - %d", result);
    } catch (...) {
      // Handle database errors here.
      QRLogger::log(CAT_QMP, LL_ERROR, "Unknown exception - exiting.");
      exit(0);
    }

    // Delay waiting to try again for a successful SQL table read
    // Wait 3 minutes
    if (publishTarget != PUBLISH_TO_FILE) DELAY(18000);
  } while (publishTarget != PUBLISH_TO_FILE);

  QRLogger::log(CAT_QMP, LL_INFO, "MXQMP process has terminated.");
  return result;
}  // End of mainline
