package org.trafodion.dcs.master.listener;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketAddress;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.message.KeepAliveMessage;
import org.trafodion.dcs.master.message.ReplyMessage;
import org.trafodion.dcs.zookeeper.ZkClient;

public class RequestKeepAliveCheck {

    private static final Logger LOG = LoggerFactory.getLogger(RequestCancelQuery.class);

    private ZkClient zkc;
    private String parentDcsZnode;

    public RequestKeepAliveCheck(ConfigReader configReader){
        this.zkc = configReader.getZkc();
        this.parentDcsZnode = configReader.getParentDcsZnode();
    }

    public void processRequest(Data data) {
        if (LOG.isInfoEnabled()) {
            LOG.info("ENTRY. Request KeepAlive Check...");
        }
        boolean aliveCheckConnection = false;
        SocketAddress s = data.getClientSocketAddress();
        KeepAliveMessage keepAliveMessage = data.getKeepAliveMessage();

        try {
            // get input
            String dialogueIds = keepAliveMessage.getDialogueId();
            if (LOG.isDebugEnabled()) {
                LOG.debug("<{}>. dialogueId : <{}>", s, dialogueIds);
            }
            // process request
            List<Integer> list = new ArrayList<>();
            if (dialogueIds != null && dialogueIds.length() > 0) {

                String[] ids = dialogueIds.split(",");
                for (String str : ids) {
                    list.add(Integer.parseInt(str.trim()));
                }
                String registeredPath =
                    parentDcsZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED;
                String nodeRegisteredPath = "";
                List<String> servers = null;
                Stat stat = null;
                String nodeData = "";

                if (!registeredPath.startsWith("/")) {
                    registeredPath = "/" + registeredPath;
                }

                zkc.sync(registeredPath, null, null);
                servers = zkc.getChildren(registeredPath, null);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Request KeepAlive Check. registeredPath: <{}>, servers: <{}>",
                            registeredPath, servers);
                }
                if (!servers.isEmpty()) {
                    for (String server : servers) {
                        nodeRegisteredPath = registeredPath + "/" + server;
                        stat = zkc.exists(nodeRegisteredPath, false);
                        if (stat != null) {
                            nodeData = new String(zkc.getData(nodeRegisteredPath, false, stat));
                            if (nodeData.startsWith("CONNECTED") || nodeData.startsWith("RESTART") || nodeData.startsWith("DISABLE") || nodeData.startsWith("REBALANCE")) {
                                String[] stData = nodeData.split(":");
                                if (list.contains(Integer.parseInt(stData[2]))) {
                                    list.remove((Object) Integer.parseInt(stData[2]));
                                }
                            }
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Request KeepAlive Check. nodeData: <{}>", nodeData);
                            }
                        } else {
                            if (LOG.isInfoEnabled()) {
                                LOG.info(
                                        "Request KeepAlive Check, Stat is null. nodeRegisteredPath: <{}>",
                                        nodeRegisteredPath);
                            }
                        }
                    }
                }
            }

            // build output
            ReplyMessage reply = new ReplyMessage(list);
            data.setReplyMessage(reply);
        } catch (UnsupportedEncodingException ue){
            LOG.error("RequestKeepAliveCheck.UnsupportedEncodingException: <{}> : <{}>.", s, ue.getMessage());
            aliveCheckConnection = true;
        } catch (KeeperException ke){
            LOG.error("RequestKeepAliveCheck.KeeperException: <{}> : <{}>.", s, ke.getMessage());
            aliveCheckConnection = true;
        } catch (InterruptedException ie){
            LOG.error("RequestKeepAliveCheck.InterruptedException: <{}> : <{}>.", s, ie.getMessage());
            aliveCheckConnection = true;
        } catch (BufferUnderflowException e){
            LOG.error("RequestKeepAliveCheck.BufferUnderflowException: <{}> : <{}>.", s, e.getMessage());
            aliveCheckConnection = true;
        } catch (IOException e) {
            LOG.error("RequestKeepAliveCheck.IOException: <{}> : <{}>.", s, e.getMessage());
            aliveCheckConnection = true;
        }catch (Exception e){
            LOG.error("Unknown Exception: <{}> : <{}>.", s, e.getMessage());
            aliveCheckConnection = true;
        }

        if (aliveCheckConnection) {
            data.setRequestReply(ListenerConstants.REQUST_CLOSE);
        } else {
            data.setRequestReply(ListenerConstants.REQUST_WRITE);
        }
    }

}
