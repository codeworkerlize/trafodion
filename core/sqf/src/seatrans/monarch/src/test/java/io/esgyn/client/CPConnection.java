package io.esgyn.client;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;

public class CPConnection {

    public static MClientCache createClientCache(ParseArgs p_pa) {

        MConfiguration mconf = MConfiguration.create();
        mconf.set(Constants.MonarchLocator.MONARCH_LOCATOR_ADDRESS, p_pa.m_locator_host);
        mconf.setInt(Constants.MonarchLocator.MONARCH_LOCATOR_PORT, p_pa.m_locator_port);
        mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, p_pa.m_log_file_name);
        MClientCache clientCache = new MClientCacheFactory().create(mconf);
	
	return clientCache;
    }

}
