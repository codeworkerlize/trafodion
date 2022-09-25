package org.trafodion.jdbc.t4.utils;

public class Utils {
    private static String lineSeparator;
    static {
        lineSeparator = java.security.AccessController.doPrivileged(
                new sun.security.action.GetPropertyAction("line.separator"));
    }
    public static String lineSeparator(){
        return lineSeparator;
    }

    public static String parsingUrl(String address) {
        String tmpAddress = address
                .replace("jdbc:t4jdbc://", "");
        if (tmpAddress.indexOf("/") > -1) {
            tmpAddress = tmpAddress.substring(0, tmpAddress.indexOf("/"));
        }
        return tmpAddress;
    }
}
