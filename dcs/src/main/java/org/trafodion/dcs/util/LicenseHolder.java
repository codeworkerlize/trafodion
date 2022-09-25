package org.trafodion.dcs.util;

import com.esgyn.common.LicenseHelper;

public class LicenseHolder {

    private static boolean isMultiTenancyEnabled = false;

    static {
        isMultiTenancyEnabled = LicenseHelper.isMultiTenancyEnabled()&&LicenseHelper.isModuleOpen(LicenseHelper.Modules.MULTITENANCY);
    }

    public static boolean isMultiTenancyEnabled() {
        return isMultiTenancyEnabled;
    }

}
