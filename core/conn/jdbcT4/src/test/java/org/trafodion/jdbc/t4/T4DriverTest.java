/*
 * @@@ START COPYRIGHT @@@
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @@@ END COPYRIGHT @@@
 */

package org.trafodion.jdbc.t4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.util.Task;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class T4DriverTest {

    private Logger logger = LoggerFactory.getLogger(T4DriverTest.class);
    private static AtomicInteger counter = new AtomicInteger();

    private static T4Driver driver;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Class.forName("org.trafodion.jdbc.t4.T4Driver");
    }


    @Test
    public void acceptsURL() throws SQLException {
        String url = "jdbc:t4jdbc://localhost:23400/:";
        Assert.assertTrue(driver.acceptsURL(url));
        url = "jdbc:abc://localhost:23400/:";
        Assert.assertFalse(driver.acceptsURL(url));
    }

    @Test
    public void singleThread() throws SQLException {
        int threadNum = 1000;
//        final String url = "jdbc:t4jdbc://10.10.10.8:23400/:";
//        final String url = "jdbc:t4jdbc://localhost:23400/:";
        final String url = "jdbc:t4jdbc://10.11.40.75:23400/:";
        final String user = "trafodion";
        final String pwd = "traf123";
        long s = System.currentTimeMillis();
        long end;
        for (int i = 0; i < threadNum; i++) {
            Connection conn = DriverManager.getConnection(url, user, pwd);
            end = System.currentTimeMillis();
            System.out.println("[" + i + "]" + (end - s) / 1000 + ", " + ((TrafT4Connection) conn)
                .getRemoteProcess());
        }
        System.out.println("-----");
    }


    @Test
    public void testConnection() {

        int threadNum = 1000;
//        final String url = "jdbc:t4jdbc://10.10.10.8:23400/:";
//        final String url = "jdbc:t4jdbc://localhost:23400/:";
        final String url = "jdbc:t4jdbc://10.11.40.75:23400/:";
        final String user = "trafodion";
        final String pwd = "traf123";
        final long s = System.currentTimeMillis();
        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            pool.submit(new Task() {
                @Override
                public void call() throws Exception {
                    try {

                        while (true) {
                            Connection conn = DriverManager.getConnection(url, user, pwd);
                            long end = System.currentTimeMillis();
                            System.out
                                .println(counter.addAndGet(1) + ":" + ((TrafT4Connection) conn)
                                    .getRemoteProcess() + ", Ellapse: " + (end - s) / 1000);
                            Thread.sleep(300000);
                            conn.close();
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(0);
                    }
                }
            });
        }

        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testPrepare() throws SQLException, InterruptedException {
        final String urls[] = new String[]{
//                "jdbc:t4jdbc://localhost:23400,10.13.30.141:23400/:",
            "jdbc:t4jdbc://localhost:23400,10.9.0.220:23400/:",
//                "jdbc:t4jdbc://localhost:23400,10.10.10.8:23400/:",
//                "jdbc:t4jdbc://10.10.10.8:23400/:"
//                "jdbc:t4jdbc://10.13.30.137:23400/:"
            "jdbc:t4jdbc://10.9.0.220:23400/:"
        };
        int numThreads = 1;
        Thread[] threads = new Thread[numThreads];
        for (int index = 0; index < numThreads; index++) {

            threads[index] = new Thread() {
                @Override
                public void run() {
                    int urlIndex = Math.abs(new Random().nextInt(2));
                    String url = urls[0];
//                    String user = "trafodion";
                    String user = "db__root";
                    String pwd = "traf123";
                    int n = 0;
                    while (true) {
                        ++n;
                        long start = System.currentTimeMillis();
                        try {
                            Connection conn = DriverManager.getConnection(url, user, pwd);
                            System.out.println(n + "----:" + Thread.currentThread().getId() + ","
                                + ((TrafT4Connection) conn).getRemoteProcess() + ", url=" + url);
                            for (int i = 0; i < 1; i++) {
                                PreparedStatement st = conn.prepareStatement("select * from dual");
                                ResultSet rs = st.executeQuery();
                                while (rs.next()) {
                                }
                                st.close();
                            }
                            conn.close();

                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(
                                new Date() + " - " + n + "---" + Thread.currentThread().getId()
                                    + ","
                                    + "--err--" + e.getMessage() + ", url=" + url);
                            System.exit(0);
                        } finally {
                            System.out.println(n + "+++++++:" + Thread.currentThread().getId() + ","
                                + ", " + (System.currentTimeMillis() - start) + "ms] urlIndex="
                                + urlIndex);
                        }
                    }
                }
            };
            threads[index].start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

}
