// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package org.trafodion.sql;
import org.apache.log4j.Logger;
import javax.servlet.http.HttpServletResponse;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import java.net.*;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;


class StrawScanUtils {

	static Object globalLock = new Object();
    static Logger logger = Logger.getLogger(StrawScanUtils.class.getName());
    static String[] littleJettyHosts;
    static int port;
    static{
    	//Configuration conf = DcsConfiguration.create();
    	Configuration conf = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
    	port = conf.getInt("org.trafodion.sql.littlejetty.port", 8680);
    	//override port for worstation builds. the env variable bellow is onluy set on worstation builds
        String s = System.getenv("MY_LITTLE_JETTY_PORT");
        try {
            if (s != null && !s.equals(""))
               port = Integer.parseInt(s);
          } 
          catch (Exception e) {
            logger.error(String.format("Failed to parse: %s as int for littleJetty port" , s));            
          }
        logger.debug(String.format("LittleJetty is on port %d",port));
    }
    
    private static void setupLittleJettyHosts(String webservers){
    	if (littleJettyHosts==null){
    		synchronized(globalLock){
    			if (littleJettyHosts==null){
    				String[] stringList = webservers.split(",");
    				for (int i=0; i< stringList.length; i++){
    					stringList[i] = String.format("%s:%d",stringList[i], port);
    				}
    				littleJettyHosts = stringList;
    			}
    		}
    	}
    }
    
	//implement some deterministic load balance between all littleJetty servers
	private static String getHost(String queryId, int explainNodeId){
      Checksum checksum = new CRC32();
      byte[] bytes = queryId.getBytes();
      checksum.update(bytes,0,bytes.length);
      checksum.update(explainNodeId);
      long i = checksum.getValue() % littleJettyHosts.length;
      return littleJettyHosts[(int)i];
	}
    public static void initStrawScan(String webServers, String queryId, int explainNodeId, boolean isFactTable, Object[] entries) 
    throws MalformedURLException,IOException,UnsupportedEncodingException, StrawScanException{
      //group entries by filepath
      HashMap<String,StringBuilder> hm = new HashMap<String,StringBuilder>();
      for(Object entry: entries){
        String s = (String)entry;
        int i = s.indexOf('|');
        String filePath = s.substring(0,i);
        String data = s.substring(i+1);
        StringBuilder sb = hm.get(filePath);
        if (sb == null){
            sb = new StringBuilder();
            hm.put(filePath,sb);
        }
        sb.append(data).append('\n');
      }//end for
      //construct final POST content
      //first line contains isFactTable param
      StringBuilder result = new StringBuilder(1000000);
      result.append(isFactTable?"isFactTable=true\n":"isFactTable=false\n");
      Iterator<Map.Entry<String,StringBuilder>> it = hm.entrySet().iterator();
      while (it.hasNext()){
    	  Map.Entry<String,StringBuilder> es = it.next();
    	  result.append('h').append(es.getKey()).append('\n').append(es.getValue().toString());
      }

      //setup http post
      setupLittleJettyHosts(webServers);
      String url = String.format("http://%s/strawscan/create/%s-%d",getHost(queryId,explainNodeId),queryId,explainNodeId);
  	  URLConnection connection = new URL(url).openConnection();
      HttpURLConnection httpConnection = (HttpURLConnection)connection;
      httpConnection.setRequestMethod("POST");
      connection.setDoOutput(true);
  	  try(OutputStream output  = connection.getOutputStream()){
  		output.write(result.toString().getBytes("UTF-8"));
  	  }
  	  InputStream response = connection.getInputStream();  	
  	  int status = httpConnection.getResponseCode();
      if (logger.isDebugEnabled()) logger.debug(String.format("initStrawScan queryId=%s, explainNodeId=%d, isFactTable=%s, url:%s, httpResponse:%d",
                                                              queryId, explainNodeId, isFactTable?"true":"false", url, status));   
      if (status != HttpServletResponse.SC_CREATED)
         throw new StrawScanException("Fail to create littleJetty container");
    }

    public static int getNextRangeNumStrawScan(String webServers, String queryId, int explainNodeId, int sequenceNb, long executionCount, int espNb, int nodeId, boolean isFactTable)
    throws IOException{
      setupLittleJettyHosts(webServers);
      String url = String.format("http://%s/strawscan/pull/%s-%d?sequenceNb=%d&espNb=%d&nodeId=%d&isFactTable=%s&executionId=%d",
    		  getHost(queryId,explainNodeId),queryId,explainNodeId,
    		  sequenceNb, espNb,nodeId,isFactTable?"true":"false",executionCount);
      URLConnection connection = new URL(url).openConnection();
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine;	
      String result=null;	
      while((inputLine = in.readLine()) != null)
         result=inputLine;
      in.close();	
      if (logger.isDebugEnabled()) logger.debug(
    		  String.format("getNextRangeNumStrawScan queryId=%s, explainNodeId=%d, sequenceNb=%d, executionCount=%d, espNb=%d, nodeId=%d, isFactTable=%s, url:%s, result:%s",
    		  queryId, explainNodeId, sequenceNb, executionCount, espNb, nodeId, isFactTable, url, result));
      if (result.equals("QUEUE_EMPTY"))
    	  return -1;
      else
   	      return Integer.parseInt(result.split("\\|")[1]); 
    }
     
    //release littleJetty memory consumed by queryId/explainNodeId.
    public static void freeStrawScan(String queryId, int explainNodeId)throws IOException{
        String url = String.format("http://%s/strawscan/free/%s-%d",getHost(queryId,explainNodeId),queryId,explainNodeId);
        if (logger.isDebugEnabled()) logger.debug(String.format("freeStrawScan queryId=%s, explainNodeId=%d, url:%s",queryId,explainNodeId,url));   
     	URLConnection connection = new URL(url).openConnection();
     	BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
     	String inputLine;	
     	String result=null;	
     	while((inputLine = in.readLine()) != null)
     		result=inputLine;
     	in.close();     	
       	return;
    }
}