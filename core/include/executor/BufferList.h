// **********************************************************************

// **********************************************************************

// BufferList.h
//
// BufferList is a buffer list that combines buffer management with a
// Queue.  BufferList does not own the buffers in its list.
//

#ifndef BUFFERLIST_H
#define BUFFERLIST_H

#include "comexe/ComQueue.h"
#include "common/NAType.h"
#include "executor/ExOverflow.h"
#include "export/NABasicObject.h"

namespace ExOverflow {

class BufferList : public NABasicObject {
 public:
  BufferList(NAMemory *heap);
  ~BufferList(void);

  // Return a pointer to the last buffer in the list
  char *back(void);

  // Return a pointer to the current buffer in the list
  char *current(void);

  // Is the buffer list empty?
  bool empty(void);

  // Return a pointer to the first buffer in the list
  char *front(void);

  // Return a pointer to the next buffer in the list
  char *next(void);

  // Unlink the first buffer from the list
  char *pop(void);

  // Reset current position to head of list
  void position(void);

  // Insert a buffer at the end of the list
  void push_back(char *buffer);

  // Number of buffers in the buffer list
  UInt32 size(void);

 private:
  Queue bufferList_;  // List of buffers
};

}  // namespace ExOverflow

#endif
