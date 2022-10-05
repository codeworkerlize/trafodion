
#ifndef EXECUTE_ID_TRIGGERS_H
#define EXECUTE_ID_TRIGGERS_H
//
// -- struct ExecuteId
//
// This structure uniquely identifies a specific trigger execution instance at
// run time. It is created by the executor root, and is recorded in the trigger
// temporary table.

typedef struct {
  int cpuNum;          // cpu number in the cluster
  int pid;             // process id
  void *rootTcbAddress;  // root tcb address
} ExecuteId;

// This constant is used by the Catman when it creates the temp table
const int SIZEOF_UNIQUE_EXECUTE_ID = sizeof(ExecuteId);
#endif  // EXECUTE_ID_TRIGGERS_H
