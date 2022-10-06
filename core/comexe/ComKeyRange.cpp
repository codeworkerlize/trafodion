
#include "comexe/ComKeyRange.h"

#include "comexe/ComKeyMDAM.h"
#include "comexe/ComKeySingleSubset.h"
#include "comexe/ComPackDefs.h"
#include "exp/ExpCriDesc.h"

keyRangeGen::keyRangeGen(key_type keyType, int keyLen, ex_cri_desc *workCriDesc, unsigned short keyValuesAtpIndex,
                         unsigned short excludeFlagAtpIndex, unsigned short dataConvErrorFlagAtpIndex)
    : keyType_(keyType),
      keyLength_(keyLen),
      workCriDesc_(workCriDesc),
      keyValuesAtpIndex_(keyValuesAtpIndex),
      excludeFlagAtpIndex_(excludeFlagAtpIndex),
      dataConvErrorFlagAtpIndex_(dataConvErrorFlagAtpIndex),
      NAVersionedObject(keyType),
      keytag_(0),
      flags_(0) {
  for (int i = 0; i < FILLER_LENGTH; i++) fillersKeyRangeGen_[i] = '\0';
}

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID; used by NAVersionedObject::driveUnpack().
// -----------------------------------------------------------------------
char *keyRangeGen::findVTblPtr(short classID) {
  char *vtblPtr;
  switch (classID) {
    case KEYSINGLESUBSET:
      GetVTblPtr(vtblPtr, keySingleSubsetGen);
      break;
    case KEYMDAM:
      GetVTblPtr(vtblPtr, keyMdamGen);
      break;
    default:
      GetVTblPtr(vtblPtr, keyRangeGen);
      break;
  }
  return vtblPtr;
}

void keyRangeGen::fixupVTblPtr() {
  switch (getType()) {
    case KEYSINGLESUBSET: {
      keySingleSubsetGen bek;

      COPY_KEY_VTBL_PTR((char *)&bek, (char *)this);
    } break;
    case KEYMDAM: {
      keyMdamGen mdam;

      COPY_KEY_VTBL_PTR((char *)&mdam, (char *)this);
    } break;
    default:
      break;
  }
}

Long keyRangeGen::pack(void *space) {
  workCriDesc_.pack(space);
  return NAVersionedObject::pack(space);
}

int keyRangeGen::unpack(void *base, void *reallocator) {
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
