#ifndef NAMESSAGETYPE_H
#define NAMESSAGETYPE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         NAMessageType.h
 * Description:  Associate numbers with SQL/ARK message types
 *
 *
 * Created:      10/17/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// This file defines simple data types used for exchanging messages
// between processes. Each message has a message header and a sequence of
// zero or more other message objects in it (the header is a message
// object itself). Each object that a user wants to add to a message
// needs to be a class (or struct, or union), derived from a base
// class, NAMessageObject. That base class adds certain information to
// the object, namely:
//
// - an object type (can be used to call the correct constructor at
//   the receiving side, since we can't automatically send virtual
//   function pointers over the net)
// - an object version, to allow interaction between processes of
//   different releases
// - an object length
// - a reference count, used to count the users of an object that is
//   shared by multiple components or processes
//
// This file describes the basic types used for the above information:
//
// NAMessageType             Type of a message header
// NAMessageObjType          Type of a non-header object in the message
// NAMessageObjVersion       Version of a message object
// NAMessageObjSize          Size of a message object
// NAMessageObjRefCount      Reference count to a message object
//
// Furthermore, some additional types to identify network domains
// and data representation across different hardware platforms are
// declared here as well. Note that not all of the hardware platforms
// are actually supported by the current implementation.
// -----------------------------------------------------------------------

#include "common/Platform.h"

// -----------------------------------------------------------------------
// We want to support interoperation, so most data structures can
// handle both Guardian and Unix-style personalities. An enumeration
// type tells which communication protocol we are using.
// -----------------------------------------------------------------------
typedef enum NANetworkDomainEnum { NA_DOM_INVALID, NA_DOM_GUA_PHANDLE, NA_DOM_INTERNET } NANetworkDomain;

// -----------------------------------------------------------------------
// Types of server processes supported (see file ExIPC.C for the
// logic that associates a name with each literal)
// -----------------------------------------------------------------------
typedef enum NAServerTypeEnum { NA_SQLCAT_SERVER, NA_SQLCOMP_SERVER, NA_SQLESP_SERVER } NAServerType;

// -----------------------------------------------------------------------
// Message types used by this protocol.
//
// Rather than recompiling everything when this changes, one could also
// reserve ranges of numbers in this file and manage them separately.
// -----------------------------------------------------------------------
typedef enum NAMessageTypeEnum {
  NA_MSG_GUA_CLOSE = -999,
  NA_MSG_GUA_STARTUP = 1,

  // messages to and from the SQL catalog manager
  // for the actual message types see file ???
  NA_MSG_SQLCAT_FIRST = 2000,
  NA_MSG_SQLCAT_LAST = 2999,

  // messages to and from the SQL compiler process
  // for the actual mesage types see file ???
  NA_MSG_SQLCOMP_FIRST = 3000,
  NA_MSG_SQLCOMP_LAST = 3999,

  // messages to and from SQL ESP processes
  // for the actual message types see file ???
  NA_MSG_SQLESP_FIRST = 4000,
  NA_MSG_SQLEXP_LAST = 4999,

  // messages to and from the SQL shadow process from the CLI (Windows/NT only)
  // for the actual message types see file ???
  NA_MSG_SQLCLI_FIRST = 5000,
  NA_MSG_SQLCLI_LAST = 5999,

  // misc. message types, used mainly for testing and debugging
  NA_MSG_INVALID = 6000,
  NA_MSG_TEST = 6001,
  NA_MSG_TEST_REPLY = 6002

} NAMessageType;

// -----------------------------------------------------------------------
// Types for objects inside messages (a different space of numbers for
// each message type, if so desired). Procedures that deal with message
// types and message object types all take parameters of type
// NAMessageObjType, since enum types are non-portable. This also
// allows us to manage the enum types separately per component of the
// SQL system without forcing global recompiles on small changes.
// -----------------------------------------------------------------------
typedef int NAMessageObjType;

// -----------------------------------------------------------------------
// Version of a message header or of an object in a message
// -----------------------------------------------------------------------
typedef int NAMessageObjVersion;

// -----------------------------------------------------------------------
// size of objects in bytes
// -----------------------------------------------------------------------

typedef int NAMessageObjSize;

// -----------------------------------------------------------------------
// Reference count of a message header or of an object in a message
// (used mainly when objects are shared across the IPC interface)
// -----------------------------------------------------------------------
typedef int NAMessageRefCount;

// -----------------------------------------------------------------------
// A buffer pointer to a raw, byte-adressable  message buffer, used for
// packing and unpacking of objects contained in message buffers
// -----------------------------------------------------------------------
typedef char *NAMessageBufferPtr;
typedef const char *NAConstMessageBufferPtr;

// -----------------------------------------------------------------------
// Endianness (define literals for all options and for what this
// process is using)
// -----------------------------------------------------------------------

const short NALittleEndian = 1;
const short NABigEndian = 2;

#ifdef NA_LITTLE_ENDIAN
const short NAMyEndianness = NALittleEndian;
#else
const short NAMyEndianness = NABigEndian;
#endif

// -----------------------------------------------------------------------
// Data alignment
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Alignment8 means that bytes are on any address, 16 bit numbers
// are on even addresses, 32 bit numbers are on addresses divisible
// by 4, and 64 bit numbers are on addresses divisible by 8.
// -----------------------------------------------------------------------
const short NAAlignment8 = 8;

// -----------------------------------------------------------------------
// right now, everybody is using Alignment8 and our message structures
// are laid out to have no fillers when this alignment is used
// -----------------------------------------------------------------------
const short NAMyAlignment = NAAlignment8;

#endif /* NAMESSAGETYPE_H */
