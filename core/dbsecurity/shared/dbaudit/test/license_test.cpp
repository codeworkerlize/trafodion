#include <stdlib.h>
#include <gtest/gtest.h>
#include "common/sq_license.h"
#include "../../../../sqf/monitor/linux/licensecommon.h"

static char cmd[4 * 1024] = {0};
const char *trafHome = getenv("TRAF_HOME");
static char* licenseFilePath = "/tmp/test_license";

/*
    $TRAF_HOME/../dbsecurity/license/encoder -v 2 -t Internal -p ADV -f 5 -c EsgynTest -n 1 -m 16376 -e
    $(( $(date +%s ) / (24 * 3600) + 150 )) > esgyn_license
*/
#define MAKE_LICENSE(flag)                                                                                    \
    {                                                                                                         \
        sprintf(cmd, "%s/../dbsecurity/license/encoder -v 2 -t Internal -p ADV -f 5 -c EsgynTest -n 1 -m %d " \
"-e $(( $(date +%%s ) / (24 * 3600) + 150 )) > %s && sync",                                                   \
                trafHome,                                                                                     \
                1 << (flag - 1),                                                                              \
                licenseFilePath);                                                                             \
        if (-1 == system(cmd))                                                                                \
            abort();                                                                                          \
    }

int main()
{

    //cd $TRAF_HOME
    chdir("/tmp");
    if (0 != setenv("SQ_MON_LICENSE_FILE", licenseFilePath, 1))
        abort();

    MAKE_LICENSE(LM_ROW_LOCK);
    {
        CLicenseCommon lic;
        EXPECT_NE(lic.isModuleOpen(LM_SEC_AUDIT), true);
        EXPECT_EQ(lic.isModuleOpen(LM_ROW_LOCK), true);
        EXPECT_NE(lic.isModuleOpen(LM_SEC_MAC), true);
        EXPECT_NE(lic.isModuleOpen(LM_LOCAL_AUTH), true);
    }

    MAKE_LICENSE(LM_LOCAL_AUTH);
    {
        CLicenseCommon lic;
        EXPECT_NE(lic.isModuleOpen(LM_SEC_AUDIT), true);
        EXPECT_NE(lic.isModuleOpen(LM_ROW_LOCK), true);
        EXPECT_NE(lic.isModuleOpen(LM_SEC_MAC), true);
        EXPECT_EQ(lic.isModuleOpen(LM_LOCAL_AUTH), true);
    }

    return 0;
}