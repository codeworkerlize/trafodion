package org.apache.hadoop.hbase.client.transactional;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.client.transactional.utils.BashScript;

public class AsyncXDCManager implements Runnable {
  public static int WAIT_INCREMENTAL_MS = 1000;
  static final Log LOG = LogFactory.getLog(AsyncXDCManager.class);

  private STRConfig        str_conf;
  private Configuration    local_hbase_conf;
  private boolean          running = true;

  private final ArrayList<String> worker_hosts = new ArrayList<String>();


  public static void setupConf() {
    String confFile = System.getProperty("trafodion.log4j.configFile");
    if (confFile == null) {
      System.setProperty("hostName", System.getenv("HOSTNAME"));
      System.setProperty("trafodion.atrxdc.log", System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.manager.log");
      confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
    }
    PropertyConfigurator.configure(confFile);
  }

  public AsyncXDCManager() {
    new Thread(this).start();
  }

  // TODO: read worker hosts from the other place.
  private void readWorkerConfig() throws IOException {

    String servers_file_name = System.getenv("TRAF_CONF") + "/dcs/servers";
    try (BufferedReader br = new BufferedReader(new FileReader(servers_file_name))) {
      worker_hosts.clear();

      String line;
      while ((line = br.readLine()) != null) {
        Scanner scn = new Scanner(line);
        scn.useDelimiter("\\s+");
        String hostName = null;
        hostName = scn.next();// host name
        scn.close();

        worker_hosts.add(hostName);
      }

      if (worker_hosts.size() < 1)
        throw new IOException("No entries found in servers file");

      LOG.info("AsyncXDCManger configured worker node(s): " + worker_hosts);
    }
  }

  public static void main(String[] args) {
    setupConf();
    new AsyncXDCManager();
  }

  @Override
  public void run() {
    LOG.debug("creating local configuration");
    local_hbase_conf = HBaseConfiguration.create();

    try {
      // get STRConfig instance
      LOG.debug("getting STRConfig instance");
      str_conf = STRConfig.getInstance(local_hbase_conf);

      // reader worker nodes config
      readWorkerConfig();

      // connecting to master hbase
      LOG.debug("try connecting to master hbase to make sure we can connect to primary cluster");
      Configuration master_hbase_conf = str_conf.getPeerConfiguration(str_conf.getFirstRemotePeerId());
      if (master_hbase_conf == null) {
        LOG.error("AsyncXDCManager can't get primary cluster config, try to run 'xdc -list' to check you configuration");
        return;
      }

      Connection master_conn = ConnectionFactory.createConnection(master_hbase_conf);
      master_conn.close();

      // run workers
      try(ATRConfig master_atrconf = ATRConfig.newInstance(master_hbase_conf)) {
        int binlog_partition_num = Integer.parseInt(master_atrconf.getBinlogPartNum());
        ArrayList<BashScript> start_scripts = new ArrayList<>(binlog_partition_num);
        for (int i = 0; i < binlog_partition_num; ++i) {
          BashScript start_worker = new BashScript(String.format("atrxdc start worker %s %d %d", worker_hosts.get(i % worker_hosts.size()), i, binlog_partition_num));
          LOG.debug("start worker with command: " + start_worker);
          start_worker.execute();
          start_scripts.add(start_worker);
        }

        // watch workers
        do {
          for (BashScript s : start_scripts) {
            if (s.is_finished()) {
              LOG.error("AsyncXDCManager worker exited, try to restart, start command is: " + s.toString());
              s.execute();
            }
          }
          TimeUnit.SECONDS.sleep(1);
        } while (running);
      }
      catch (KeeperException e) {
        LOG.error("AsyncXDCManager get master ATRConfig error:", e);
      }

      LOG.info("AsyncXDCManager doesn't continue running");
    } catch (ZooKeeperConnectionException e) {
      LOG.error("Zookeeper Connection Error: ", e);
    } catch (IOException e) {
      LOG.error("IO Error: ", e);
    } catch (InterruptedException e) {
      LOG.error("AsyncXDCManager interrupted: ", e);
    }
    LOG.info("AsyncXDCManager exit");
  }
}
