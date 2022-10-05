
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLQualName.cpp
 * Description:  an element representing a qualified name
 *					used as a node in a list of qualified names
 *
 * Created:      06/20/99
 * Language:     C++
 * Project:		 MV refresh groups ( OZ ) & general use
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLQualName.h"

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

ElemDDLQualName::ElemDDLQualName(const QualifiedName &qualName) : qualName_(qualName, PARSERHEAP()) {}

ElemDDLQualName::~ElemDDLQualName() {}

ElemDDLQualName *ElemDDLQualName::castToElemDDLQualName() { return this; }
