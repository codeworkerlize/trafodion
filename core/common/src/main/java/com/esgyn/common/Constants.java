// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

public class Constants {

    /** Default client port that the zookeeper listens on */
    public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;
    
    public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/trafodion";
    

    public static final String ZOOKEEPER_QUORUM = "zookeeper.quorum";
    
    /** Configuration key for ZooKeeper session timeout */
    public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";

    /** Default value for ZooKeeper session timeout */
    public static final int DEFAULT_ZK_SESSION_TIMEOUT = 180 * 1000;
    
    /** Configuration key for ZooKeeper recovery retry */
    public static final String ZK_RECOVERY_RETRY = "zookeeper.recovery.retry";
    
    /** Default value for ZooKeeper recovery retry */
    public static final int DEFAULT_ZK_RECOVERY_RETRY = 3;
    
    /** Configuration key for ZooKeeper recovery retry interval millis */
    public static final String ZK_RECOVERY_RETRY_INTERVAL_MILLIS = "zookeeper.recovery.retry.intervalmillis";
    
    /** Default value for ZooKeeper recovery retry interval millis */
    public static final int DEFAULT_ZK_RECOVERY_RETRY_INTERVAL_MILLIS = 1000;

}
