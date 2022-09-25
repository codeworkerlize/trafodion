package org.trafodion.jdbc.t4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.trafodion.jdbc.t4.h2Cache.CacheUtil;

public class CacheTest {

  public static void main(String[] args)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.trafodion.jdbc.t4.T4Driver");
    int m = 1;
    Thread[] ts = new Thread[m];
    for (int n = 0; n < m; n++) {
      ts[n] = new Thread() {
        @Override
        public void run() {
          try {
            String tableName = "t1";
            String url = "jdbc:t4jdbc://192.168.100.204:23400/:schema=seabase;maxCachedRows=3;cacheRefreshIntervalSecs=100;cacheTable=" + tableName;
//            String url = "jdbc:t4jdbc://192.168.100.204:23400/:";
            String user = "db__root";
            String pwd = "traf123";
            Connection trafconn = DriverManager.getConnection(url, user, pwd);
//            PreparedStatement ps = trafconn.prepareStatement("select * from t3 where 1=?");
            PreparedStatement ps = trafconn
                .prepareStatement("select * from " + tableName + " where 1=?");
            System.out.println("------------" + ps);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
//              System.out.println(i + "------------");
              ps.setInt(1, 1);
              ResultSet rs = ps.executeQuery();
              int rows = 0;
              while (rs.next()) {
//                System.out.println(Thread.currentThread().getName()+", "+i+": "+rs.getObject(1));
                if (++rows > 10) {
                  break;
                }
              }
              rs.close();
//              System.out.println("Num of rows: " +rows);
            }
            System.out.println(
                Thread.currentThread().getId() + " Ellapse time: " + (System.currentTimeMillis()
                    - start));

//    String sql = "select * from t1 a, t1 b";
//    TrafCache cache = TrafCache.ins();
//    cache.cache("t1,t2", trafconn);
//    if (cache.check(sql)) {
//      System.out.println("get from cache " + sql);
//    }

            trafconn.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

      };
      ts[n].start();
    }


  }

  public static void main1(String[] args) {
    String ddl =
        "    PRODUCTCODE                      VARCHAR(10 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , MSGTYPE                          VARCHAR(100 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLNUM                           VARCHAR(3 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLNAME                          VARCHAR(200 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLFLAGE                         VARCHAR(3 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , COLDEFAULT                       VARCHAR(100 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , COLTYPE                          VARCHAR(50 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLLENGTH                        VARCHAR(4 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , COLSCALE                         VARCHAR(10 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , COLDESC                          VARCHAR(300 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , COLMUST                          VARCHAR(1 CHAR) CHARACTER SET UTF8 COLLATE\n"
            + "      DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLMAPNAME                       VARCHAR(30 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT NO DEFAULT NOT NULL NOT DROPPABLE NOT SERIALIZED\n"
            + "  , COLPROCESS                       VARCHAR(150 CHARS) CHARACTER SET UTF8\n"
            + "      COLLATE DEFAULT DEFAULT NULL NOT SERIALIZED\n"
            + "  , PRIMARY KEY (PRODUCTCODE ASC, MSGTYPE ASC, COLNUM ASC)\n";
    String finalSql = CacheUtil.transDDL("aaaaaaa", ddl);
    System.out.println(finalSql);
  }
}
