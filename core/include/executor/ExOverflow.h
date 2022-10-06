// **********************************************************************

// **********************************************************************
//
// ExOverflow.h - ExOverflow module common declarations

#ifndef EXOVERFLOW_H
#define EXOVERFLOW_H

#include "export/NAVersionedObject.h"

namespace ExOverflow {
enum IoStatus { OK, IO_PENDING, SQL_ERROR, END_OF_DATA, IO_ERROR, NO_MEMORY, INTERNAL_ERROR };

const UInt32 ONE_MEGABYTE = 1024 * 1024;

// ExOverflow module common types
typedef UInt32 BufferCount;
typedef UInt32 BufferOffset;
typedef UInt32 ByteCount;
typedef UInt64 ByteCount64;
typedef UInt64 SpaceOffset;
}  // namespace ExOverflow

#endif
