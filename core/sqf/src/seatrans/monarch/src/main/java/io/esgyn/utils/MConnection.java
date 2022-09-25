package io.esgyn.utils;

import io.ampool.conf.Constants;
import io.ampool.monarch.table.*;
import io.ampool.monarch.table.client.MClientCache;
import io.ampool.monarch.table.client.MClientCacheFactory;
import io.ampool.monarch.table.Admin;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;


public class MConnection {

    static MConfiguration sv_mconf = null;
    static MClientCache sv_clientCache = null;
    static String sv_logFile = null;
    static Logger LOG = Logger.getLogger(MConnection.class.getName());

    static {
       String confFile = System.getProperty("trafodion.log4j.configFile");
       if (confFile == null) {
	  System.setProperty("hostName", System.getenv("HOSTNAME"));
          sv_logFile = System.getenv("TRAF_LOG") + "/trafodion.sql.java.${hostName}.log";
          System.setProperty("trafodion.sql.log", sv_logFile);
          confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
       }
       PropertyConfigurator.configure(confFile);
       sv_mconf = MConfiguration.create();
       sv_mconf.addResource("ampool-site.xml");
       sv_mconf.set(Constants.MClientCacheconfig.ENABLE_META_REGION_CACHING, "true");
       sv_mconf.set(Constants.MClientCacheconfig.MONARCH_CLIENT_LOG, sv_logFile);
       sv_clientCache = MClientCacheFactory.getOrCreate(sv_mconf);
   }

   public static MConfiguration getMConfiguration() {
      return sv_mconf;
   }

   public static MTable getTable(String tableName) 
   {
      return sv_clientCache.getTable(tableName);
   }

   public static Admin getAdmin() 
   {
      return sv_clientCache.getAdmin();
   }
}
