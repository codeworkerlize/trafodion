
#ifndef FIXUP_VTABLE_H
#define PFIXUP_VTABLE_H

class QualifiedName;
class NAString;
class HTableCache;
class HTableRow;

// fixupVTable() copies the virtual table pointer (1st 8 bytes)
// in a C++ onject , from a valid source object to target object.
// The target object can be on shared cache where its vurtual
// table object pointer is created by another process and therefore
// invalid in the current process.
//
// General versions that do nothing.
void fixupVTable(void *);
void fixupVTable(const void *);

// Special versions created for table descriptor shared cache
// that does the pointer copy.
void fixupVTable(QualifiedName *);
void fixupVTable(NAString *);
void fixupVTable(HTableCache *);
void fixupVTable(HTableRow *);

#endif
