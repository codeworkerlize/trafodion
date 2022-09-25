// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common.util;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryCounter {
  private static final Logger LOG = LoggerFactory.getLogger(RetryCounter.class);
  private final int maxRetries;
  private int retriesRemaining;
  private final int retryIntervalMillis;
  private final TimeUnit timeUnit;

  public RetryCounter(int maxRetries, 
  int retryIntervalMillis, TimeUnit timeUnit) {
    this.maxRetries = maxRetries;
    this.retriesRemaining = maxRetries;
    this.retryIntervalMillis = retryIntervalMillis;
    this.timeUnit = timeUnit;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  /**
   * Sleep for a exponentially back off time
   * @throws InterruptedException
   */
  public void sleepUntilNextRetry() throws InterruptedException {
    int attempts = getAttemptTimes();
    long sleepTime = (long) (retryIntervalMillis * Math.pow(2, attempts));
    LOG.info("Sleeping " + sleepTime + "ms before retry #" + attempts + "...");
    timeUnit.sleep(sleepTime);
  }

  public boolean shouldRetry() {
    return retriesRemaining > 0;
  }

  public void useRetry() {
    retriesRemaining--;
  }
  
  public int getAttemptTimes() {
    return maxRetries-retriesRemaining+1;
  }
}
