
/* -*-C++-*-
******************************************************************************
*
* File:         LmJavaOptions.cpp
* Description:  A container class for JVM option strings
*
* Created:      June 2003
* Language:     C++
*
*
******************************************************************************
*/

#include "LmJavaOptions.h"

#include "LmAssert.h"
#include "LmDebug.h"
#include "common/ComSmallDefs.h"
#include "common/NAMemory.h"
#include "common/str.h"
#include "langman/LmCommon.h"

//----------------------------------------------------------------------
// LmJavaOptions methods
//----------------------------------------------------------------------
LmJavaOptions::LmJavaOptions() : options_(collHeap()) {}

LmJavaOptions::~LmJavaOptions() {
  int e = options_.entries();
  for (int i = 0; i < e; i++) {
    NADELETEBASIC(options_[i], collHeap());
  }
}

int LmJavaOptions::entries() const { return options_.entries(); }

const char *LmJavaOptions::getOption(int i) const { return options_[i]; }

void LmJavaOptions::addOption(const char *option, NABoolean trim) {
  if (option == NULL || option[0] == '\0') {
    return;
  }

  char *toBeDeleted = NULL;
  if (trim) {
    toBeDeleted = copy_string(collHeap(), option);
    option = strip_spaces(toBeDeleted);
  }

  char *s = copy_string(collHeap(), option);
  options_.insert(s);

  if (toBeDeleted) {
    NADELETEBASIC(toBeDeleted, collHeap());
  }
}

void LmJavaOptions::removeOption(int index) {
  NADELETEBASIC(options_[index], collHeap());
  options_.removeAt(index);
}

void LmJavaOptions::removeAllOptions() {
  int e = options_.entries();
  for (int i = 0; i < e; i++) {
    NADELETEBASIC(options_[i], collHeap());
  }
  options_.clear();
}

void LmJavaOptions::addOptions(const char *options, const char *delimiters, NABoolean trim) {
  if (options == NULL || options[0] == '\0') {
    return;
  }

  LM_ASSERT(delimiters != NULL);

  char *copy = copy_string(collHeap(), options);

  for (char *tok = strtok(copy, delimiters); tok != NULL; tok = strtok(NULL, delimiters)) {
    if (trim) {
      strip_spaces(tok);
    }
    addOption(tok, FALSE);
  }

  NADELETEBASIC(copy, collHeap());
}

CollIndex LmJavaOptions::findByPrefix(const char *prefix) const {
  size_t prefixLen = strlen(prefix);

  for (CollIndex i = 0; i < options_.entries(); i++)
    if (strncmp(options_[i], prefix, prefixLen) == 0) return i;

  return NULL_COLL_INDEX;
}

void LmJavaOptions::addSystemProperty(const char *name, const char *value) {
  if (name == NULL || name[0] == '\0') {
    return;
  }

  if (value == NULL) {
    value = "";
  }

  removeSystemProperty(name, NULL, NULL);

  const char *format = "-D%s=%s";
  const int len1 = 3;               // the "-D" and "=" characters
  const int len2 = str_len(name);   // the name string
  const int len3 = str_len(value);  // the value string

  char *option = new (collHeap()) char[len1 + len2 + len3 + 1];
  str_sprintf(option, format, name, value);

  options_.insert(option);
}

void LmJavaOptions::display() {
  LM_DEBUG0("[BEGIN LmJavaOptions]");

  int e = entries();
  for (int i = 0; i < e; i++) {
    const char *option = getOption(i);
    LM_DEBUG1("  '%s'", option);
  }

  LM_DEBUG0("[END LmJavaOptions]");
}

//
// Remove all assignments for a given property from the option set. A
// caller can request the property value from the rightmost "-D<name>"
// option by passing non-NULL values for callersOutputPointer and
// callersHeap.
//
NABoolean LmJavaOptions::removeSystemProperty(const char *name, char **callersOutputPointer, NAMemory *callersHeap) {
  return getSystemProperty(name, callersOutputPointer, callersHeap, true);
}

// Get the assignment for a given system property from the option set.
// A caller can request the property value from the rightmost "-D<name>"
// option by passing non-NULL values for callersOutputPointer and
// callersHeap.
//
NABoolean LmJavaOptions::getSystemProperty(const char *name, char **callersOutputPointer, NAMemory *callersHeap,
                                           NABoolean remove) {
  if (name == NULL || name[0] == '\0') {
    return FALSE;
  }

  NABoolean callerWantsValue = FALSE;
  if (callersOutputPointer) {
    LM_ASSERT(callersHeap);
    callerWantsValue = TRUE;
  }

  NABoolean found = FALSE;
  char *valueToReturn = NULL;

  const int nameLen = str_len(name);
  const int prefixLen = nameLen + 2;
  const int prefixWithEqualsLen = nameLen + 3;
  char *prefix = new (collHeap()) char[3 + nameLen + 1];
  str_cat("-D", (char *)name, prefix);
  str_cat(prefix, "=", prefix);

  int i = entries();
  while (i--) {
    const char *option = getOption(i);

    // Anything that begins with "-D<name>" needs special
    // attention. If the entire option is "-D<name>" or "-D<name>=",
    // that means the user wants no value for this property. Otherwise
    // if there is a value after the equals sign we need to pass that
    // value back to our caller, but only if this is the rightmost
    // occurence of "-D<name>".

    if (str_cmp(option, prefix, (int)prefixLen) == 0) {
      const UInt32 optionLen = str_len(option);

      if (optionLen == prefixLen) {
        // This option is simply "-D<name>" without the equals sign
        found = TRUE;
        if (callerWantsValue && !valueToReturn) {
          valueToReturn = new (callersHeap) char[1];
          valueToReturn[0] = '\0';
        }
        if (remove) removeOption(i);
      }

      if (str_cmp(option, prefix, (int)prefixWithEqualsLen) == 0) {
        // This option begins with "-D<name>="
        if (optionLen <= prefixWithEqualsLen) {
          // Nothing appears after the equals sign
          found = TRUE;
          if (callerWantsValue && !valueToReturn) {
            valueToReturn = new (callersHeap) char[1];
            valueToReturn[0] = '\0';
          }
          if (remove) removeOption(i);
        } else {
          // Something appears after the equals sign
          found = TRUE;
          if (callerWantsValue && !valueToReturn) {
            const int valueLen = optionLen - prefixWithEqualsLen;
            valueToReturn = new (callersHeap) char[valueLen + 1];
            str_cpy_all(valueToReturn, &option[prefixWithEqualsLen], (int)(valueLen + 1));
          }
          if (remove) removeOption(i);
        }
      }

    }  // if option prefix is "-D<name>"
  }    // for each option

  NADELETEBASIC(prefix, collHeap());

  if (found == TRUE) {
    if (callerWantsValue) {
      LM_ASSERT(valueToReturn);
      *callersOutputPointer = valueToReturn;
    }
  } else {
    LM_ASSERT(!valueToReturn);
  }

  return found;
}

//
// Append all options from other to this
//
void LmJavaOptions::merge(const LmJavaOptions &other) {
  const int numOptions = other.entries();
  for (int i = 0; i < numOptions; i++) {
    const char *option = other.getOption(i);
    addOption(option, FALSE);
  }
}
