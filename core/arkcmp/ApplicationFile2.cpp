
#define _XOPEN_SOURCE_EXTENDED 1
#include <stdlib.h>

#include "ApplicationFile.h"
#include "common/DgBaseType.h"
#include "export/ComDiags.h"
#include "mxCompileUserModule.h"

// create a temporary file and open it for write.
FILE *ApplicationFile::createTempFile(char *nam, char *newNam) {
  // There is a risk of a race condition here:
  // "tmpnam, in Application::getTempFileName(), guarantees the name it returns
  // isn't in use now and that tmpnam won't returnn same name again in the this
  // process.  We take that name and add some a suffix (process num).  tmpnam's
  // guaranteee that the file didn't exist is not of use since we use it as
  // the root of our filename.  That is, if tmpnam returns "TMPxyz" and we add
  // the process number and turn it into "TMPxyz987654", there is no guarantee
  // that "TMPxyz987654" doesn't already exist.  Also race condition issue
  // even for base name.  Perhaps use mkstemp instead?  Creates file & takes a
  // 'template' for name --  you could provide template with process num and/or
  // timestamp frag & some identifying chars in it for extra uniqueness &
  // identifiability.  I made this major because there was no retry code and
  // the result of a collision could be nasty."

  // OK. mkstemp does what we want here. You'd think simply calling mkstemp
  // here and we're done. Not so. It turns out that c89 & nld are sensitive
  // to mkstemp's function signature. mkstemp is declared in <stdlib.h>
  // guarded by #if _XOPEN_SOURCE_EXTENDED == 1
  // If we add these 2 lines
  //   #define _XOPEN_SOURCE_EXTENDED 1
  //   #include <stdlib.h>
  // to ApplicationFile.h or ApplicationFile.cpp, c89 rejects the mkstemp()
  // call in ApplicationFile::createTempFile as an undeclared identifier.
  // Attempts to do an in-line declaration of mkstemp as
  //   int    mkstemp(char *);
  // seem to pass c89 & nld, but running mxCompileUserModule on OSS results
  // in an unresolved reference to mkstemp & an INSPECT session:
  //      usr/treyes/bin/debug: mxCompileUserModule
  //      PID: \SQUAW.0,423 mxCompileUserModule (ELF)
  //      External References Not Resolved to Any User/System Library:
  //      Prg: mxCompileUserModule -> mkstemp__FPc (PROC)

  // It appears that a negative interaction of #include's and #define's
  // and c89's various phases conspire to prevent the mkstemp() call from
  // being resolved to its declaration in <stdlib.h> and its definition in
  // zosshsrl and libossh.srl.

  // Splitting ApplicationFile::createTempFile's definition away into its
  // own source file (ApplicationFile2.cpp) seems to clear up this negative
  // interaction. Sigh :-( These dozen or so lines took 3 days to fix :-(

  // Since mxCompileUserModule can potentially be used to sql compile lots,
  // possibly thousands, of sql modules, we must work around mkstemp's 26
  // unique temporary file name limit. We do this by using tmpnam in
  // ApplicationFile::getTempFileName to compose up to TMP_MAX (27,576)
  // different temporary file name prefixes.

  strcpy(newNam, nam);
  // NT has no mkstemp(), so just fall thru and create & open temp file
  FILE *tFil = fopen(newNam, "w");
  if (!tFil) {
    *mxCUMptr << FAIL << DgSqlCode(-2206) << DgString0(newNam);
  }
  return tFil;
}
