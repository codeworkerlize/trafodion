// **********************************************************************

// **********************************************************************

#include "comexe/ComTdbProbeCache.h"

#include "comexe/ComTdbCommon.h"

// Dummy constructor for "unpack" routines.
ComTdbProbeCache::ComTdbProbeCache() : ComTdb(ComTdb::ex_PROBE_CACHE, eye_PROBE_CACHE), tuppIndex_(0){};

// Constructor

ComTdbProbeCache::ComTdbProbeCache(ex_expr *hash_probe_expr, ex_expr *encode_probe_expr, ex_expr *move_inner_expr,
                                   ex_expr *select_pred, int probe_len, int inner_rec_len, int cache_size,
                                   const unsigned short tupp_index, const unsigned short hashValIdx,
                                   const unsigned short encodedProbeDataIdx, const unsigned short innerRowDataIdx,
                                   ComTdb *child_tdb, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                                   queue_index down, queue_index up, Cardinality estimatedRowCount, int numInnerTuples)
    : ComTdb(ComTdb::ex_PROBE_CACHE, eye_PROBE_CACHE, estimatedRowCount, given_cri_desc, returned_cri_desc, down, up,
             0,   // num_buffers - we use numInnerTuples_ instead.
             0),  // buffer_size - we use numInnerTuples_ instead.
      tdbChild_(child_tdb),
      hashProbeExpr_(hash_probe_expr),
      encodeProbeExpr_(encode_probe_expr),
      moveInnerExpr_(move_inner_expr),
      selectPred_(select_pred),
      probeLen_(probe_len),
      cacheSize_(cache_size),
      recLen_(inner_rec_len),
      numInnerTuples_(numInnerTuples),
      tuppIndex_(tupp_index),
      hashValIdx_(hashValIdx),
      encodedProbeDataIdx_(encodedProbeDataIdx),
      innerRowDataIdx_(innerRowDataIdx),
      probeCacheFlags_(0){};

ComTdbProbeCache::~ComTdbProbeCache(){};

void ComTdbProbeCache::display() const {};

Long ComTdbProbeCache::pack(void *space) {
  tdbChild_.pack(space);
  hashProbeExpr_.pack(space);
  encodeProbeExpr_.pack(space);
  moveInnerExpr_.pack(space);
  selectPred_.pack(space);
  return ComTdb::pack(space);
}

int ComTdbProbeCache::unpack(void *base, void *reallocator) {
  if (tdbChild_.unpack(base, reallocator)) return -1;
  if (hashProbeExpr_.unpack(base, reallocator)) return -1;
  if (encodeProbeExpr_.unpack(base, reallocator)) return -1;
  if (moveInnerExpr_.unpack(base, reallocator)) return -1;
  if (selectPred_.unpack(base, reallocator)) return -1;
  return ComTdb::unpack(base, reallocator);
}

void ComTdbProbeCache::displayContents(Space *space, int flag) {
  ComTdb::displayContents(space, flag & 0xFFFFFFFE);

  if (flag & 0x00000008) {
    char buf[200];

    str_sprintf(buf, "\nFor ComTdbProbeCache :");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "probeLen_ = %d, cacheSize_ = %d, recLen_ = %d, tuppIndex_ = %d", probeLen_, cacheSize_, recLen_,
                tuppIndex_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, "probeCacheFlags_ = %x, numInnerTuples_ =%d", probeCacheFlags_, numInnerTuples_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (flag & 0x00000001) {
    displayExpression(space, flag);
    displayChildren(space, flag);
  }
}

int ComTdbProbeCache::orderedQueueProtocol() const { return 1; }
