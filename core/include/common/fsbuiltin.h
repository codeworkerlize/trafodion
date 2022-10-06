
// PREPROC: start of file: file guard
#if (!defined(fsbuiltin_included_already) || defined(fsbuiltin_including_section) || defined(fsbuiltin_including_self))
//
//
#if (!defined(fsbuiltin_including_self) && !defined(fsbuiltin_including_section))
#define fsbuiltin_included_already
#endif
//
//
// PREPROC: start of section:
#if (defined(fsbuiltin_) || (!defined(fsbuiltin_including_section) && !defined(fsbuiltin_including_self)))
#undef fsbuiltin_
//
#include "fs/adds/fsbuiltins.h"

//
#endif
// PREPROC: end of section:
//
//
#if (!defined(fsbuiltin_including_self))
#undef fsbuiltin_including_section
#endif

#endif  // file guard
// end of file
