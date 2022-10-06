
#ifndef COM_MEMORY_DIAGS_H
#define COM_MEMORY_DIAGS_H
/* -*-C++-*-
****************************************************************************
*
* File:         ComMemoryDiags.h
* RCS:          $Id: ComMemoryDiags.h,v 1.1 2007/10/09 19:38:52  Exp $
* Description:
*
* Created:      7/15/1998
* Modified:     $ $Date: 2007/10/09 19:38:52 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
**************************************************************************** */

// -----------------------------------------------------------------------
//  The following classes are defined in this file.
// -----------------------------------------------------------------------
class ComMemoryDiags;  // used to diagnose memory leaks

#include <iosfwd>
using namespace std;

//
// class ComMemoryDiags is simply a wrapper around the static data member
//
//      ostream * dumpMemoryInfo_
//
// This data member refers to a filestream where we dump memory information
// at various stages of compilation.  The resulting file is used to debug
// memory leak / usage information (this is a vital component of the effort
// to reduce/remove memory leaks in arkcmp!).
//
// We're doing this for two reasons : we need to enforce the non-interdependence
// of cli.lib and tdm_arkcmp.exe; also, we'd like to minimize pollution of the global
// namespace.
//
// This class (data member) is used in two places :
//
//   /cli/Context.cpp
//   /sqlcomp/CmpMain.cpp
//
// The static data member is initialized in ComMemoryDiags.cpp.
//

class ComMemoryDiags {
 public:
  static ostream *&DumpMemoryInfo() { return dumpMemoryInfo_; }

 private:
  static ostream *dumpMemoryInfo_;
};

#endif /* COM_MEMORY_DIAGS_H */
