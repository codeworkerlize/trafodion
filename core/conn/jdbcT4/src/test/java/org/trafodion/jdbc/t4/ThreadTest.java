package org.trafodion.jdbc.t4;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadTest {

  public static void main(String[] args) {
    ThreadPoolExecutor tpe = null;
    try {
//      BlockingQueue<Runnable> bq = new ArrayBlockingQueue<Runnable>(0);
      BlockingQueue<Runnable> bq = new LinkedBlockingQueue<Runnable>(0);
      // ThreadPoolExecutor:创建自定义线程池，池中保存的线程数为3，允许最大的线程数为6
      tpe = new ThreadPoolExecutor(3, 6, 50, TimeUnit.MILLISECONDS, bq);

      for (int i = 0; i < 1000; i++) {
        tpe.execute(new TempThread(i));
      }
    } finally {
      // 关闭自定义线程池
      tpe.shutdown();
    }
  }

  private static class TempThread implements Runnable {

    private final int id;

    public TempThread(int id) {
      this.id = id;
    }

    @Override
    public void run() {
      System.out.println(String
          .format("Execute thread id: %s, task id: %s, start time: %s",
              Thread.currentThread().getId(), this.id, new Date()));
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
