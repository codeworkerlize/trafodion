
#include "comexe/ComTdbConnectByTempTable.h"

#include "comexe/ComQueue.h"
#include "comexe/ComTdbCommon.h"
#include "common/str.h"

ComTdbConnectByTempTable::ComTdbConnectByTempTable()
    : ComTdb(ComTdb::ex_CONNECT_BY_TEMP_TABLE, eye_CONNECT_BY_TEMP_TABLE), tdbChild_(NULL) {}

ComTdbConnectByTempTable::ComTdbConnectByTempTable(
    ComTdb *child_tdb, ex_expr *hash_probe_expr, ex_expr *encode_probe_expr, ex_expr *move_inner_expr, int probe_len,
    int inner_rec_len, int cache_size, const unsigned short tupp_index, const unsigned short hashValIdx,
    const unsigned short encodedProbeDataIdx, const unsigned short innerRowDataIdx, ex_cri_desc *workCriDesc,
    ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up, int numBuffers,
    int bufferSize, ex_expr *encodeInputHostVarExpr, ex_expr *hvExprInput, UInt16 hashInputValIdx,
    UInt16 encodeInputProbeDataIdx, ex_expr *scanExpr)
    : ComTdb(ComTdb::ex_CONNECT_BY_TEMP_TABLE, eye_CONNECT_BY_TEMP_TABLE, 0, givenCriDesc, returnedCriDesc, down, up,
             numBuffers, bufferSize),
      tdbChild_(child_tdb),
      hashProbeExpr_(hash_probe_expr),
      encodeProbeExpr_(encode_probe_expr),
      moveInnerExpr_(move_inner_expr),
      probeLen_(probe_len),
      cacheSize_(cache_size),
      recLen_(inner_rec_len),
      tuppIndex_(tupp_index),
      hashValIdx_(hashValIdx),
      encodedProbeDataIdx_(encodedProbeDataIdx),
      innerRowDataIdx_(innerRowDataIdx),
      workCriDesc_(workCriDesc),
      hashInputExpr_(hvExprInput),
      encodeInputExpr_(encodeInputHostVarExpr),
      hashInputValIdx_(hashInputValIdx),
      encodeInputProbeDataIdx_(encodeInputProbeDataIdx),
      scanExpr_(scanExpr) {}

ComTdbConnectByTempTable::~ComTdbConnectByTempTable() {}

void ComTdbConnectByTempTable::display() const {};

Long ComTdbConnectByTempTable::pack(void *space) {
  tdbChild_.pack(space);
  workCriDesc_.pack(space);
  hashProbeExpr_.pack(space);
  encodeProbeExpr_.pack(space);
  moveInnerExpr_.pack(space);
  hashInputExpr_.pack(space);
  encodeInputExpr_.pack(space);
  scanExpr_.pack(space);

  return ComTdb::pack(space);
}

int ComTdbConnectByTempTable::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  if (workCriDesc_.unpack(base, reallocator)) return -1;
  if (hashProbeExpr_.unpack(base, reallocator)) return -1;
  if (encodeProbeExpr_.unpack(base, reallocator)) return -1;
  if (moveInnerExpr_.unpack(base, reallocator)) return -1;
  if (hashInputExpr_.unpack(base, reallocator)) return -1;
  if (encodeInputExpr_.unpack(base, reallocator)) return -1;
  if (scanExpr_.unpack(base, reallocator)) return -1;

  return ComTdb::unpack(base, reallocator);
}

void ComTdbConnectByTempTable::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[100];
    str_sprintf(buf, "\nFor ComTdbConnectByTempTable :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}
