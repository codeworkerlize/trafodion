#ifndef GEN_RESOURCES_H
#define GEN_RESOURCES_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         GenResources.h
 * Description:  Code to retrieve resource-related defaults from the defaults
 *               table and to add them to a structure in the generated
 *               plan.
 *
 *
 * Created:      1/9/99
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------
class Generator;
class ExScratchFileOptions;
#define MAX_SCRATCH_LOCATIONS 32  // keep in sync with STFS_stub::MAX_STFS_LOCATIONS

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------

ExScratchFileOptions *genScratchFileOptions(Generator *generator);

#endif /* GEN_RESOURCES_H */
