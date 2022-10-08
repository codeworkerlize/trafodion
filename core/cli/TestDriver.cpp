
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         TestDriver.cpp
 * Description:  Driver for testing the new CLI calls.
 *
 *
 * Created:      7/18/00
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

// #include "cli/cli_stdh.h"
// #include "cli/Cli.h"

#include "cli/sqlcli.h"
#include "cli/cli_stdh.h"
#include "stdio.h"
#include "stdlib.h"
#include "time.h"

static int contextTest();

int main() {
  // Create a Procedure

  // Prepare a CALL statement

  // Describe Input

  // Describe Output

  // Execute the CALL statement prepared

  return 0;
}

int contextTest() {
#define NUM_OF_CTX  8
#define NUM_OF_ITER 16

  SQLCTX_HANDLE ctxVec[NUM_OF_CTX];

  char idVec[NUM_OF_CTX][32];

  for (int i = 0; i < NUM_OF_CTX; i++) {
    char *authId = &idVec[i][0];

    itoa(i % 4 + 1000, authId, 10);

    int code = SQL_EXEC_CreateContext(&ctxVec[i], authId);

    if (code != SUCCESS) {
      cout << "Error in Creating Context:" << i;

      exit(-1);
    }
  }

  for (i = 0; i < NUM_OF_ITER; i++) {
    srand((UInt32)time(NULL));

    int hdNum = rand() % NUM_OF_CTX;

    SQLCTX_HANDLE currHandle = 0;

    SQLCTX_HANDLE prevHandle = 0;

    int code = SQL_EXEC_CurrentContext(&currHandle);

    if (code != SUCCESS) {
      cout << "Error in Obtaining Current Context";

      exit(-1);
    }

    code = SQL_EXEC_SwitchContext(ctxVec[hdNum], &prevHandle);

    if (code != SUCCESS) {
      cout << "Error in Switching to Context:" << ctxVec[hdNum];
    }

    if (prevHandle != currHandle) {
      cout << "Error in ContextManagement, Inconsistency";

      exit(-1);
    }
  }

  for (i = 0; i < NUM_OF_ITER; i++) {
    srand((UInt32)time(NULL));

    int hdNum = rand() % NUM_OF_CTX;

    int code = SQL_EXEC_DeleteContext(ctxVec[hdNum]);

    if (code != SUCCESS) {
      cout << "Error in Deleting Context:" << ctxVec[hdNum];

      exit(-1);
    }
  }
  return 0;
}
