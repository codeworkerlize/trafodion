package org.apache.hadoop.hbase.coprocessor.transactional.lock.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IPUtils {
    private final static String COMPUTERNAME = "COMPUTERNAME";

    public static String hostName;

    public static String getHostNameForLinux() {
        if (hostName != null) {
            return hostName;
        }
        try {
            hostName = (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage(); // host = "hostname: hostname"
            if (host != null) {
                int colon = host.indexOf(':');
                if (colon > 0) {
                    hostName = host.substring(0, colon);
                }
            }
            return null;
        }
        return hostName;
    }

    public static String getHostName() {
        if (hostName != null) {
            return hostName;
        }
        if (System.getenv(COMPUTERNAME) != null) {
            hostName = System.getenv(COMPUTERNAME);
        } else {
            hostName = getHostNameForLinux();
        }
        return hostName;
    }

    public static void main(String[] args) {
        System.out.println(getHostName());
    }
}
