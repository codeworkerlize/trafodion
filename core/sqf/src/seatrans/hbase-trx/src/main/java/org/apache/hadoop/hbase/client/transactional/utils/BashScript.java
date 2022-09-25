package org.apache.hadoop.hbase.client.transactional.utils;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BashScript {
  public String cmd;
  private Process p;

  public BashScript(String command) {
    cmd = command;
  }

  public void execute() throws IOException {
    String[] execStrings = new String[] { "bash", "-c", cmd };
    p = Runtime.getRuntime().exec(execStrings);
  }

  public boolean is_finished() throws InterruptedException {
    return p.waitFor(0, TimeUnit.SECONDS);
  }

  public int exit_code() throws InterruptedException {
    return p.waitFor();
  }

  public String stdout() throws InterruptedException, IOException {
    p.waitFor();
    return new BufferedReader(new InputStreamReader(p.getInputStream())).lines().collect(Collectors.joining("\n"));
  }

  public String stderr() throws InterruptedException, IOException {
    p.waitFor();
    return new BufferedReader(new InputStreamReader(p.getErrorStream())).lines().collect(Collectors.joining("\n"));
  }

  public String get_execution_context() {
    return String.format("(user: %s), (command: %s), (path: %s)",
                         System.getenv().get("USER"),
                         this.cmd,
                         System.getenv().get("PATH"));
  }

  public String toString() {
    return cmd;
  }
}
