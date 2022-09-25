package org.trafodion.dcs.master.listener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.mapping.DefinedMapping;
import org.trafodion.dcs.master.registeredServers.RegisteredServers;
import org.trafodion.dcs.util.DcsConfiguration;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.zookeeper.ZkClient;

public class ConfigReader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigReader.class);
    private Configuration conf;

    private File file;
    private String fullPath;
    private long timeStamp;

    private String hostSelectionMode = "";
    private boolean userAffinity = true;
    private int masterSyncCacheCheckTimes;
    private String enableSSL = "false";

    private Map<String, Map<String, String>> ipMappingDatas =
            new HashMap<String, Map<String, String>>();
    private long ipConfigFileModifyTimestamp = 0L;
    private String ipConfigFilePath;
    private String defaultIpMapping;

    private String parentDcsZnode;
    private String parentWmsZnode;

    private String trafInstanceId = null;

    private ZkClient zkc = null;
    private DefinedMapping mapping = null;
    private RegisteredServers registeredServers = null;
    private int encryptBase64Enable = 0;
    private static String isEmptyEqualsNull;

    public ConfigReader(Configuration conf, ZkClient zkc) {
        while (zkc.getZk() == null || zkc.getZk().getState() != ZooKeeper.States.CONNECTED) {
            try {
                Thread.sleep(10000L); // wait 10 seconds
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        this.zkc = zkc;

        this.conf = conf;
        userAffinity = this.conf.getBoolean(Constants.DCS_MASTER_USER_SERVER_AFFINITY,
                Constants.DEFAULT_DCS_MASTER_USER_SERVER_AFFINITY);
        hostSelectionMode = this.conf.get(Constants.DCS_MASTER_HOST_SELECTION_MODE,
                Constants.DEFAULT_DCS_MASTER_HOST_SELECTION_MODE);
        masterSyncCacheCheckTimes = this.conf.getInt(Constants.DCS_MASTER_SYNC_CACHE_CHECK_TIMES,
                Constants.DEFAULT_DCS_MASTER_SYNC_CACHE_CHECK_TIMES);
        defaultIpMapping = this.conf.get(Constants.DCS_DEFAULT_IP_MAPPING,
                Constants.DEFAULT_DCS_DEFAULT_IP_MAPPING);
        parentDcsZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,
                Constants.DEFAULT_ZOOKEEPER_ZNODE_DCS_PARENT);
        enableSSL = conf.get(Constants.DCS_SERVER_USER_PROGRAM_USESSL_ENABLE,
                Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_USESSL_ENABLE);
        encryptBase64Enable = conf.getInt(Constants.DCS_SERVER_PROGRAM_ENCRYPT_BASE64_ENABLE,
                Constants.DEFAULT_DCS_SERVER_PROGRAM_ENCRYPT_BASE64_ENABLE);
        trafInstanceId = System.getenv("TRAF_INSTANCE_ID");
        if (trafInstanceId != null) {
            parentWmsZnode = parentDcsZnode.substring(0, parentDcsZnode.lastIndexOf("/"));
        } else {
            parentWmsZnode = parentDcsZnode;
        }
        fileConfFile();
        try {
            this.mapping = new DefinedMapping(this);
            this.registeredServers = new RegisteredServers(this);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
        isEmptyEqualsNull = Util.isEmptyEqualsNull();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "ConfigReader. userAffinity<{}>, hostSelectionMode<{}>, masterSyncCacheCheckTimes<{}>, defaultIpMapping<{}>, parentDcsZnode<{}>, enableSSL<{}>, encryptBase64Enable<{}>, trafInstanceId<{}>, parentWmsZnode<{}>, isEmptyEqualsNull<{}>",
                    userAffinity, hostSelectionMode, masterSyncCacheCheckTimes, defaultIpMapping,
                    parentDcsZnode, enableSSL, encryptBase64Enable, trafInstanceId,
                    parentWmsZnode, isEmptyEqualsNull);
        }
    }

    public static String getIsEmptyEqualsNull() {
        return isEmptyEqualsNull;
    }

    public int getEncryptBase64Enable() {
        return encryptBase64Enable;
    }

    private void fileConfFile() {
        fullPath = GetJavaProperty.getDcsHome() + "/conf/dcs-site.xml";
        if (LOG.isDebugEnabled()) {
            LOG.debug("Conf file path : <{}>.", fullPath);
        }
        file = new File(fullPath);
        timeStamp = file.lastModified();
    }

    private boolean isConfFileUpdated() {
        long timeStamp = file.lastModified();
        if (this.timeStamp != timeStamp) {
            this.timeStamp = timeStamp;
            // Yes, file is updated
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf file is updated");
            }
            return true;
        }
        //No, file is not updated
        return false;
    }

    public String getConfHostSelectionMode() {
        if (isConfFileUpdated()) {
            this.conf = DcsConfiguration.create();
            hostSelectionMode = this.conf.get(Constants.DCS_MASTER_HOST_SELECTION_MODE,
                    Constants.DEFAULT_DCS_MASTER_HOST_SELECTION_MODE);
            if (LOG.isInfoEnabled()) {
                LOG.info("Conf hostSelectionMode changed : <{}>", hostSelectionMode);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf hostSelectionMode : <{}>", hostSelectionMode);
            }
        }
        return hostSelectionMode;
    }

    public boolean getUserAffinity() {
        if (isConfFileUpdated()) {
            this.conf = DcsConfiguration.create();
            userAffinity = this.conf.getBoolean(Constants.DCS_MASTER_USER_SERVER_AFFINITY,
                    Constants.DEFAULT_DCS_MASTER_USER_SERVER_AFFINITY);
            if (LOG.isInfoEnabled()) {
                LOG.info("Conf userAffinity changed : <{}>", userAffinity);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf userAffinity : <{}>", userAffinity);
            }
        }
        return userAffinity;
    }

    public String getEnableSSL() {
        if (isConfFileUpdated()) {
            this.conf = DcsConfiguration.create();
            enableSSL = conf.get(Constants.DCS_SERVER_USER_PROGRAM_USESSL_ENABLE,
                    Constants.DEFAULT_DCS_SERVER_USER_PROGRAM_USESSL_ENABLE);
            if (LOG.isInfoEnabled()) {
                LOG.info("Conf enableSSL changed : <{}>", enableSSL);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf enableSSL : <{}>", enableSSL);
            }
        }
        return enableSSL;
    }

    public int getMasterSyncCacheCheckTimes() {
        if (isConfFileUpdated()) {
            this.conf = DcsConfiguration.create();
            masterSyncCacheCheckTimes =
                    this.conf.getInt(Constants.DCS_MASTER_SYNC_CACHE_CHECK_TIMES,
                            Constants.DEFAULT_DCS_MASTER_SYNC_CACHE_CHECK_TIMES);
            if (LOG.isInfoEnabled()) {
                LOG.info("Conf masterSyncCacheCheckTimes changed : <{}>",
                        masterSyncCacheCheckTimes);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf masterSyncCacheCheckTimes : <{}>", masterSyncCacheCheckTimes);
            }
        }
        return masterSyncCacheCheckTimes;
    }

    public String getDefaultIpMapping() {
        if (isConfFileUpdated()) {
            this.conf = DcsConfiguration.create();
            this.defaultIpMapping = this.conf.get(Constants.DCS_DEFAULT_IP_MAPPING,
                    Constants.DEFAULT_DCS_DEFAULT_IP_MAPPING);
            if (LOG.isInfoEnabled()) {
                LOG.info("Conf defaultIpMapping changed : <{}>", defaultIpMapping);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Conf defaultIpMapping : <{}>", defaultIpMapping);
            }
        }
        return this.defaultIpMapping;
    }

    public int isForcedCloseWhitelist() {
        // 0  not Forced close
        // 1  Forced close Whitelist
        return this.conf.getInt(Constants.DCS_DEFAULT_FORCED_CLOSE_WHITELIST,
                Constants.DEFAULT_DCS_FORCED_CLOSE_WHITELIST);
    }

    public Map<String, Map<String, String>> loadIpMappingConfig() throws IOException {
        if (null == ipConfigFilePath) {
            ipConfigFilePath = GetJavaProperty.getDcsHome() + "/conf/ipmapping.conf";
        }

        File f = new File(ipConfigFilePath);
        if (!f.exists()) {
            LOG.warn("Can't find config file <{}>.", ipConfigFilePath);
            ipMappingDatas.clear();
            return ipMappingDatas;
        }
        long modifyTimestamp = f.lastModified();
        if (ipConfigFileModifyTimestamp == 0L || ipConfigFileModifyTimestamp < modifyTimestamp) {
            ipConfigFileModifyTimestamp = modifyTimestamp;

            BufferedReader bufferedReader;
            String line = null;
            try {
                bufferedReader = new BufferedReader(new FileReader(ipConfigFilePath));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading configuration file <{}>.", ipConfigFilePath);
                }
                boolean firstLine = true;
                String[] head = null;
                Map<String, String>[] mappingArr = null;

                while ((line = bufferedReader.readLine()) != null) {
                    if (line.startsWith("#") || line.trim().length() == 0) {
                        continue;
                    }
                    if (firstLine) {
                        head = line.split(",");
                        mappingArr = new Map[head.length - 1];
                        for (int i = 1; i < head.length; i++) {
                            mappingArr[i - 1] = new HashMap<String, String>();
                        }
                        firstLine = false;
                    } else {

                        String[] arr = line.split(",");
                        for (int i = 1; i < head.length; i++) {
                            // String mappingName = head[i];
                            if (i > arr.length - 1) {
                                break;
                            }
                            String inner = arr[0].trim();
                            String outer = arr[i].trim();
                            if (!isIp(inner) || !isIp(outer)) {
                                LOG.error("error ip mapping for <{}> and <{}>", inner, outer);
                                break;
                            }
                            mappingArr[i - 1].put(inner, outer);
                        }
                    }
                }
                for (int i = 1; i < head.length; i++) {
                    String mappingName = head[i].trim();
                    ipMappingDatas.put(mappingName, mappingArr[i - 1]);
                }
                bufferedReader.close();
            } catch (IOException e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Load ipMappingDatas {}.", ipMappingDatas);
        }
        return ipMappingDatas;
    }

    public static boolean isIp(String ipAddress) {
        String ip =
                "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }

    public ZkClient getZkc() {
        return zkc;
    }

    public String getParentWmsZnode() {
        return parentWmsZnode;
    }

    public String getParentDcsZnode() {
        return parentDcsZnode;
    }

    public DefinedMapping getMapping() {
        return mapping;
    }

    public RegisteredServers getRegisteredServers() {
        return registeredServers;
    }
}