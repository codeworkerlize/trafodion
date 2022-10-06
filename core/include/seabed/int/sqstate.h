//------------------------------------------------------------------
//


#ifndef __SB_INT_SQSTATE_H_
#define __SB_INT_SQSTATE_H_

//
// sqstate-ic params
//
#define INT_SQSTATE_IC_PARAMS(sre) (BMS_SRE * sre) /* sre-info */

//
// sqstate-ic name + params
//
#define INT_SQSTATE_IC_NAME_PARAMS(module, ic, sre) sqstate_ic_##module##_##ic INT_SQSTATE_IC_PARAMS(sre)

//
// sqstate-pi params
//
#define INT_SQSTATE_PI_PARAMS(node, proc, info, lib)      \
  (MS_Mon_Node_Info_Entry_Type * node, /* node-info    */ \
   MS_Mon_Process_Info_Type * proc,    /* process-info */ \
   Sqstate_Pi_Info_Type * info,        /* pi-info      */ \
   const char *lib)                    /* pi-lib       */

//
// sqstate-pi-sqstate params
//
#define INT_SQSTATE_PI_SQSTATE_PARAMS(node, proc, info, lib, rsp, rsplen, rsplenp) \
  (MS_Mon_Node_Info_Entry_Type * node, /* node-info    */                          \
   MS_Mon_Process_Info_Type * proc,    /* process-info */                          \
   Sqstate_Pi_Info_Type * info,        /* pi-info      */                          \
   const char *lib,                    /* pi-lib       */                          \
   char *rsp,                          /* pi-rsp       */                          \
   int rsplen,                         /* pi-rsplen    */                          \
   int *rsplenp)                       /* pi-rsplen    */

//
// sqstate-pi-ag params
//
#define INT_SQSTATE_PI_AG_PARAMS(info) (Sqstate_Pi_Info_Type * info) /* pi-info */

//
// pi name + params
//
#define INT_SQSTATE_PI_NAME_PARAMS(module, pi, node, proc, info, lib) \
  sqstate_pi_##module##_##pi INT_SQSTATE_PI_PARAMS(node, proc, info, lib)

//
// pi-sqstate name + params
//
#define INT_SQSTATE_PI_SQSTATE_NAME_PARAMS(module, pi, node, proc, info, lib, rsp, rsplen, rsplenp) \
  sqstate_pisqstate_##module##_##pi INT_SQSTATE_PI_SQSTATE_PARAMS(node, proc, info, lib, rsp, rsplen, rsplenp)

//
// pi-begin name + params
//
#define INT_SQSTATE_PI_AG_BEGIN_NAME_PARAMS(module, pi, info) \
  sqstate_pibegin_##module##_##pi INT_SQSTATE_PI_AG_PARAMS(info)

//
// pi-end name + params
//
#define INT_SQSTATE_PI_AG_END_NAME_PARAMS(module, pi, info) sqstate_piend_##module##_##pi INT_SQSTATE_PI_AG_PARAMS(info)

#endif  // !__SB_INT_SQSTATE_H_
