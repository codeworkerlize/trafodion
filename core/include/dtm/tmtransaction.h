
#ifndef TMTRANSACTION_H_
#define TMTRANSACTION_H_

#include "dtm/tm_util.h"
#include "dtm/tmtransid.h"

#define TMLIB_MAX_THREADS          256
#define TMLIB_MAX_TRANS_PER_THREAD 1024

// -------------------------------------------------------------------
// TM_Transaction class
// -- This object will manage transaction
//    There can be at most 1 per active transaction in a given thread
//    Transaction objects are created:
//         1)explicitly to begin a transaction
//         2) implicitly in a propagated tx
//         3) explicitly for a join
// -------------------------------------------------------------------
class TM_Transaction {
 public:
  // begin a transaction
  TM_Transaction(int abort_timeout, int64 transactiontype_bits);
  // join a transaction
  TM_Transaction(TM_Transid transid, bool propagate_tx);
  TM_Transaction();
  ~TM_Transaction();

  // transaction actions
  short end(char *&pv_err_str, int &pv_err_len, char *pv_querycontext_buf, int pv_querycontext_len);
  short abort(bool pv_doom = false);
  short suspend(TM_Transid *transid, bool coordinator_role = true);
  short resume();
  short register_region(long startid, int port, char *hostname, int hostname_length, long startcode, char *regionInfo,
                        int regionInfoLength, int peerId, int pv_tmFlags);  // TOPL
  short push_epoch(char *pa_tbldesc, int pv_tbldesc_len, char *pa_tblname, char *&pv_err_str, int &pv_err_len);
  short create_table(char *pa_tbldesc, int pv_tbldesc_len, char *pa_tblname, char **pv_keys, int pv_numsplits,
                     int pv_keylen, int pv_options, char *&pv_err_str, int &pv_err_len);
  short reg_truncateonabort(char *pa_tblname, int pv_tblname_len, char *&pv_err_str, int &pv_err_len);
  short drop_table(char *pa_tblname, int pv_tblname_len, char *&pv_err_str, int &pv_err_len);
  short alter_table(char *pa_tblname, int pv_tblname_len, char **pv_tbloptions, int pv_tbloptslen, int pv_tbloptscnt,
                    char *&pv_err_str, int &pv_err_len);
  TM_Transaction *release();
  short status(short *status);
  TM_Transid *getTransid();
  void setTransid(TM_Transid pv_transid) { iv_transid = pv_transid; }
  void setTag(unsigned int pv_tag) { iv_tag = pv_tag; }

  int getTag() { return iv_tag; }
  void setTag(int32 pv_tag) { iv_tag = pv_tag; }
  bool equal(TM_Transid pp_transid) { return ((pp_transid == iv_transid) ? true : false); }
  bool isEnder() { return iv_coordinator; }
  short get_error() { return iv_last_error; }

  static TM_Transaction *getCurrent();

  int64 getTypeFlags();
  // short setTypeFlags(int64 transactiontype_bits);

  short commitSvpt(char *&pv_err_str, int &pv_err_len, int64 pv_svpt_id, int64 pv_psvpt_id);
  short abortSvpt(int64 pv_svpt_id, int64 pv_psvpt_id);

  void setSvptId(int64 pv_svpt_id, int64 pv_psvpt_id = -1) {
    iv_svpt_id = pv_svpt_id;
    iv_psvpt_id = pv_psvpt_id;
  }
  int64 getSvptId() { return iv_svpt_id; }
  int64 getPSvptId() { return iv_psvpt_id; }

  bool getIsPropagateTX() { return iv_propagate; }

 private:
  bool iv_coordinator;
  short iv_last_error;
  bool iv_propagate;
  TM_Transid iv_transid;
  unsigned int iv_tag;
  int iv_abort_timeout;
  int64 iv_transactiontype;

  int64 iv_svpt_id;  // current savepoint id, may be inner savepoit id
  int64 iv_psvpt_id;

  short begin(int abort_timeout, int64 transactiontype_bits);
  short join(bool pv_coordinator_role);
};

#endif
