
#include "ApplicationFile.h"

#include <iostream>

#include "Cmdline_Args.h"
#include "ELFFile.h"
#include "SQLJFile.h"
#include "common/DgBaseType.h"
#include "common/NAAssert.h"
#include "export/ComDiags.h"
#include "mxCompileUserModule.h"

// factory method (or virtual constructor)
ApplicationFile *ApplicationFile::makeApplicationFile(std::string &filename) {
  // on NT/Win2K/XP, pretend it's an ELF file. SQLJ is not yet on NT.
  return new ELFFile(filename);
}

// constructor
ApplicationFile::ApplicationFile(std::string &filename)
    : fileName_(filename), nCompiles_(0), nFailures_(0), args_(NULL), appFile_(NULL) {}

// destructor
ApplicationFile::~ApplicationFile() {}

bool ApplicationFile::openFile(Cmdline_Args &args) {
  args_ = &args;
  return appFile_ != NULL;
}

// close application file
bool ApplicationFile::closeFile()  // return true if all OK
{
  bool result = fclose(appFile_) == 0;
  appFile_ = NULL;
  return result;
}

// create a temporary file name that is not the name of an existing file.
// requires: allocated length of tNam >= L_tmpnam+10.
// returns : tNam if all OK; NULL otherwise.
char *ApplicationFile::getTempFileName(char *tNam) {
  // create temporary file name
  char tempName[L_tmpnam], *tmp;
  tmp = tmpnam(tempName);
  if (!tmp) {
    *mxCUMptr << FAIL << DgSqlCode(-2205);
    return tmp;
  } else {
    // use tempName || ourProcessID as the temporary file name
    char pid[20];
    sprintf(pid, "%0d", _getpid());
    strcpy(tNam, tmp);
    strcat(tNam, pid);
    return tNam;
  }
}

// invoke mxcmp on module definition file
bool ApplicationFile::mxcmpModule(char *mdf) {
  nCompiles_++;
  char cmd[1024], *cmdP = cmd, *mxcmp = getenv("MXCMP");
#define DEFAULT_MXCMP "tdm_arkcmp"
  mxcmp = mxcmp ? mxcmp : DEFAULT_MXCMP;
  // make sure we have enough space for the mxcmp invocation string
  assert(args_ != NULL);
  int cmdLen = strlen(mxcmp) + args_->application().length() + args_->otherArgs().length() + strlen(mdf) + 7;

  if (cmdLen > 1024) {
    cmdP = new char[cmdLen];
  }
  if (!cmdP) {
    // coverity flags a REVERSE_INULL cid on cmdP -- it's complaining
    // we're not checking that cmdP is non-null. So, we oblige.
    nFailures_++;
    return false;
  }
  strcpy(cmdP, mxcmp);
  strcat(cmdP, " ");
  strcat(cmdP, args_->otherArgs().c_str());
  strcat(cmdP, " ");
  strcat(cmdP, mdf);
  cout << cmdP << endl;
  int rc = system(cmdP);
  // free any space used by mxcmp invocation string
  if (cmdP && cmdP != cmd) {
    delete cmdP;
  }
  if (rc == 0) {  // success
    if (!args_->keepMdf()) {
      remove(mdf);
    }
  } else if (rc == -1) {
    // function fails: cannot create child process
    // or cannot get child shell's exit status
    *mxCUMptr << FAIL << DgSqlCode(-2221) << DgInt0(rc);
    nFailures_++;
  } else {  // unsuccessful mxcmp invocation
    int sqlcode, int0;
    mxcmpExitCode retcode;
    int0 = rc;
    retcode = ERROR;  // cannot tell ERROR from WARNING on NT
    sqlcode = 2201;
    *mxCUMptr << retcode << DgSqlCode(-sqlcode) << DgInt0(int0);
    nFailures_++;
  }
  return rc == 0;
}

// print summary statistics
void ApplicationFile::printSummary() {
  cout << modulesFound() << " modules found, " << modulesExtracted() << " modules extracted." << endl
       << nCompiles_ << " mxcmp invocations: " << nCompiles_ - nFailures_ << " succeeded, " << nFailures_ << " failed."
       << endl;
}
