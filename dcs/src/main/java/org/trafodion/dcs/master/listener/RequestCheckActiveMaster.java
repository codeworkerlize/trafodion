package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.message.CheckActiveMasterMessage;
import org.trafodion.dcs.master.message.ReplyMessage;
import org.trafodion.dcs.script.ScriptContext;
import org.trafodion.dcs.script.ScriptManager;
import org.trafodion.dcs.util.GetJavaProperty;
import org.trafodion.dcs.zookeeper.ZkClient;

public class RequestCheckActiveMaster {

    private static final Logger LOG = LoggerFactory.getLogger(RequestCheckActiveMaster.class);

    private ZkClient zkClient = null;
    private String parentDcsZnode = "";

    public RequestCheckActiveMaster(ConfigReader configReader) {
        this.zkClient = configReader.getZkc();
        this.parentDcsZnode = configReader.getParentDcsZnode();
    }

    public void processRequest(Data data) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("ENTRY. Check Active Master...");
        }

        boolean aliveCheckConnection = false;

        SocketAddress address = data.getClientSocketAddress();
        CheckActiveMasterMessage checkActiveMasterMessage = data.getCheckActiveMasterMessage();

        try {

            String hostName = checkActiveMasterMessage.getClientHostName();
            List<String> servers = null;
            Stat stat = null;
            String nodeData = "";
            Map<String, String> map = new HashMap<>();
            StringBuilder sb = new StringBuilder();

            String leaderPath = parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER;
            String nodeleaderPath = "";

            if (!leaderPath.startsWith("/")) {
                leaderPath = "/" + leaderPath;
            }

            zkClient.sync(leaderPath, null, null);
            servers = zkClient.getChildren(leaderPath, null);
            if (!servers.isEmpty()) {
                for (String server : servers) {
                    nodeleaderPath = leaderPath + "/" + server;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("nodeleaderPath:{}", nodeleaderPath);
                    }
                    stat = zkClient.exists(nodeleaderPath, false);
                    if (stat != null) {
                        nodeData = new String(zkClient.getData(nodeleaderPath, false, stat));
                        String[] stData = nodeData.split(":");
                        //ServerName:ServerIp
                        sb.append(stData[0]);
                        sb.append(":");
                        sb.append(stData[1]);
                        sb.append(",");
                    }
                }
            }
            //default is EMPTY STRING EQUIVALENT NULL : ENABLE
            String isEmptyEqualsNull = "ENABLE";
            if(data.getHeader().getCompatibilityVersion() > 10){
                isEmptyEqualsNull = ConfigReader.getIsEmptyEqualsNull();
            }

            if (LOG.isInfoEnabled()) {
                LOG.info("ClientSocket Address:{}, HostName:{}, masterIps: <{}>", address,
                        hostName, sb.toString());
            }

            ReplyMessage reply = new ReplyMessage(sb.toString(), isEmptyEqualsNull);
            data.setReplyMessage(reply);
        } catch (InterruptedException e) {
            LOG.error("RequestCheckActiveMaster.InterruptedException: <{}> : <{}>.", address,
                    e.getMessage());
            aliveCheckConnection = true;
        } catch (IOException e) {
            LOG.error("RequestCheckActiveMaster.IOException: <{}> : <{}>.", address,
                    e.getMessage());
            aliveCheckConnection = true;
        } catch (Exception e) {
            LOG.error("RequestCheckActiveMaster.Exception: <{}> : <{}>.", address,
                    e.getMessage());
            aliveCheckConnection = true;
        }

        if (aliveCheckConnection) {
            data.setRequestReply(ListenerConstants.REQUST_CLOSE);
        } else {
            data.setRequestReply(ListenerConstants.REQUST_WRITE);
        }
    }
}
