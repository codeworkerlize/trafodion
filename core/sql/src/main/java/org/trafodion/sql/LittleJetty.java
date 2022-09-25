// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package org.trafodion.sql;
import java.util.*;
import org.eclipse.jetty.server.Server;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedReader;
import org.eclipse.jetty.server.Request;
import org.apache.hadoop.conf.Configuration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.server.handler.AbstractHandler;
import java.util.zip.CRC32;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.PropertyConfigurator;

public class LittleJetty {

    static {
	System.setProperty("hostName", System.getenv("HOSTNAME"));
    	String confFile = System.getProperty("trafodion.log4j.configFile");
    	if (confFile == null) {
    		System.setProperty("trafodion.sql.log", System.getenv("TRAF_LOG") + "/trafodion.sql.java.${hostName}.log");
    		confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
    	}
    	PropertyConfigurator.configure(confFile);
    }

    private static final Logger LOG = LoggerFactory.getLogger(LittleJetty.class);
    static private ConcurrentHashMap<String,QueueIdContext> queueIdContexts = new ConcurrentHashMap<String,QueueIdContext> ();
	static private ConcurrentHashMap<String,LinkedList<StripeOrRowgroup>> immutableQueueIds = new ConcurrentHashMap<String,LinkedList<StripeOrRowgroup>> ();
	static private ConcurrentHashMap<String,LinkedList<String>> houseKeepingForDeletes = new ConcurrentHashMap<String,LinkedList<String>>();
    static private ConcurrentHashMap<String,Long> immutableQueueIdCreationTs = new ConcurrentHashMap<String,Long> ();
	static private int memoryCleanupIntervalInMn;
        public static void main(String[] args) throws Exception {
         Configuration conf = TrafConfiguration.create(TrafConfiguration.HBASE_CONF);
         int port = conf.getInt("org.trafodion.sql.littlejetty.port", 8680);
         memoryCleanupIntervalInMn = conf.getInt("org.trafodion.sql.littlejetty.cleanupintervalmn", 1440);//default 1 day
         //override port for worstation builds. the env variable bellow is only set on worstation builds
         String s = System.getenv("MY_LITTLE_JETTY_PORT"); //for workstation build
         try {            
            if (s != null && !s.equals(""))
                port = Integer.parseInt(s);
         } 
         catch (Exception e) {
             LOG.error(String.format("Failed to parse: %s as int for littleJetty port" , s));            
         }
         LOG.debug(String.format("LittleJetty is on port %d",port));
         //start memory cleanup periodic task
         MemoryCleanupTask memoryCleanupTask = new MemoryCleanupTask();
         Timer timer = new Timer(true);
         timer.scheduleAtFixedRate(memoryCleanupTask,(long)memoryCleanupIntervalInMn * 60L * 1000L,(long)memoryCleanupIntervalInMn * 60L * 1000L);
              
         Server server = new Server(port);
         server.setAttribute("org.eclipse.jetty.server.Request.maxFormContentSize",-1);
         try {
		  ServletHandler handler = new ServletHandler();
		  server.setHandler(handler);
		  handler.addServletWithMapping(StrawScannerDequeueServlet.class,"/strawscan/pull/*");
		  handler.addServletWithMapping(StrawScannerEnqueueServlet.class,"/strawscan/create/*");
		  handler.addServletWithMapping(StrawScannerFreeResourceServlet.class,"/strawscan/free/*");
		  server.start();
	      server.join();
	     } catch (Exception e) {
	        LOG.error(String.format("Exception in LittleJetty: %s",e.toString()));
		    e.printStackTrace();
		    System.exit(0);
	     }  
        }

    public static class MemoryCleanupTask extends TimerTask {
    	@Override
    	public void run(){
    		// do the cleanup work
    		long memoryCleanupIntervalInMs = (long)memoryCleanupIntervalInMn * 60L * 1000L;
    		long nowTs = System.currentTimeMillis();
    		for (Map.Entry<String,Long> entry : immutableQueueIdCreationTs.entrySet()){
    			if (nowTs - entry.getValue() > memoryCleanupIntervalInMs){// object has not been accessed withing memoryCleanupInterval
    				  String queueId = entry.getKey();
    				  if (houseKeepingForDeletes.containsKey(queueId)){
    				    LinkedList<String> toRemove = houseKeepingForDeletes.get(queueId);
    				    for(String s : toRemove){
    					  queueIdContexts.remove(s);
    				    }
    				    houseKeepingForDeletes.remove(queueId);
    				  }
    				  immutableQueueIds.remove(queueId);  
    				  immutableQueueIdCreationTs.remove(queueId);
    				  LOG.debug(String.format("memory for QueryId reclaimed: %s",queueId));
    			}
    		}
    	}
    	
    	
    	
    }
    public static class QueueIdContext{
		LinkedList<StripeOrRowgroup> scannerQueue = new LinkedList<StripeOrRowgroup>();
		ConcurrentHashMap<String,StripeOrRowgroup> lastRowgroupSents = new ConcurrentHashMap<String,StripeOrRowgroup> ();
	}
	public static class StripeOrRowgroup{
		int sequenceNb;
		String fileName;
                int rangeNb;
		short hdfsBlockNb;		
		long offset;
		long size;
		long filteredSize;		//for future use when we implement Page Level filter using column indices for ORC or Parquet. set to size.
		short[] nodeList;
		String serializedPageList; 	//for future use when we implement Page level filter using column indices for ORC or Parquet
		String Serialize(){
			StringBuilder sb = new StringBuilder();
			sb.append(fileName).append('|')
			  .append(rangeNb).append('|')
			  .append(hdfsBlockNb).append('|')
			  .append(offset).append('|')
			  .append(size).append('|');
  		  for (int k =0; k<nodeList.length;k++){
		    if (k!=0) sb.append(' ');
		    sb.append(nodeList[k]);
		  }
		  sb.append('|').append(serializedPageList);
		  return sb.toString(); 
		}
		StripeOrRowgroup(String fileName, int rangeNb, short hdfsBlockNb, long offset, long size,long filteredSize, short[] nodeList, String serializedPageList){
			this.sequenceNb = -1;			
			this.fileName = fileName;
			this.rangeNb = rangeNb;
			this.hdfsBlockNb = hdfsBlockNb;
			this.offset = offset;
			this.size = size;
			this.filteredSize = filteredSize;
			this.nodeList = nodeList;
			this.serializedPageList = serializedPageList;
		}

	}
	public static class StrawScannerFreeResourceServlet extends HttpServlet{
		@Override
	       protected void doGet( HttpServletRequest request,
		                      HttpServletResponse response ) throws ServletException,
		                                                    IOException
		{
		  String queueId = request.getPathInfo();
		  if (houseKeepingForDeletes.containsKey(queueId)){
		    LinkedList<String> toRemove = houseKeepingForDeletes.get(queueId);
		    for(String s : toRemove){
			queueIdContexts.remove(s);
		    }
		    houseKeepingForDeletes.remove(queueId);
		  }
		  immutableQueueIds.remove(queueId);
		  immutableQueueIdCreationTs.remove(queueId);
		  response.setContentType("text/html");
		  response.setStatus(HttpServletResponse.SC_OK);
		}
	}
	public static class StrawScannerDequeueServlet extends HttpServlet{
		@Override
	       protected void doGet( HttpServletRequest request,
		                      HttpServletResponse response ) throws ServletException,
		                                                    IOException
		{
		   int sequenceNb = Integer.parseInt(request.getParameter("sequenceNb"));
		   String queueId = request.getPathInfo();
		   String queueIdExecId =  String.format("%s-%s",queueId,request.getParameter("executionId"));
		   String espNb = request.getParameter("espNb");
		   short nodeId = Short.parseShort(request.getParameter("nodeId"));
		   boolean isFactTable = "true".equals(request.getParameter("isFactTable"));
		   QueueIdContext context=null;
		   if (!queueIdContexts.containsKey(queueIdExecId)){// here we are the first get request by the first esp ready to submit it.
				        // so we need to create a context, and forbid any other get for the same queueIdExecId to do the same creation
			LinkedList<StripeOrRowgroup> semaphore = immutableQueueIds.get(queueId);
			if (semaphore == null){
		    	  response.setContentType("text/html");
			  response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		          LOG.error(String.format("context was null for queueid %s sequenceNb %d",queueId,sequenceNb));
			  response.getWriter().println(String.format("queueid was null for queueid %s sequenceNb %d",queueId,sequenceNb));
			  return;
			}
			synchronized(semaphore){
			   //check if the shallow copy was not created by another ESP			   
			   if (!queueIdContexts.containsKey(queueIdExecId)){//here we know that we are the designated ESP to do the shallow copy
				QueueIdContext queueIdContext = new QueueIdContext();
				queueIdContext.scannerQueue = (LinkedList<StripeOrRowgroup>) semaphore.clone();//do the shallow copy
				queueIdContexts.put(queueIdExecId,queueIdContext);
				context = queueIdContext;				
				//update houseKeepingForDeletes so that we can free all resources latter without doing iterator over a concurrent hashmap.
				LinkedList<String> hk;				
				if (houseKeepingForDeletes.containsKey(queueId))
				 	hk = houseKeepingForDeletes.get(queueId);
				else{
					hk = new LinkedList<String>();
					houseKeepingForDeletes.put(queueId,hk);
				}
				hk.add(queueIdExecId); // maintain the list of shallow copies for future deletes				
			   }else{
				context = queueIdContexts.get(queueIdExecId);
			   }	
			}//end synchronized		
		   }else
		      context = queueIdContexts.get(queueIdExecId);
		    StripeOrRowgroup lastRowgroup = context.lastRowgroupSents.get(espNb);

		    if (lastRowgroup != null && lastRowgroup.sequenceNb == sequenceNb)
			response.getWriter().println(lastRowgroup.Serialize());
		    else{
			LinkedList<StripeOrRowgroup> theQueue = context.scannerQueue;
			if (theQueue == null){//should never happen
		    	  response.setContentType("text/html");
			  response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			  LOG.error(String.format("theQueue was null for queueid %s sequenceNb %d",queueId,sequenceNb));
			  response.getWriter().println(String.format("thequeue was null for queueid %s sequenceNb %d",queueId,sequenceNb));
			  return;
			  }//end if
			synchronized(theQueue){			
			    immutableQueueIdCreationTs.put(queueId,System.currentTimeMillis()); //time stamp creation date to allow future memory cleanup
				Iterator<StripeOrRowgroup> iter; 
				boolean notFound = true;
				int j;
				//fact table: try to minimize disk cache trashing by having determinitic node for block locality so that same block get served
				//by only one node, therefore occupying only disk cache on one node
				//try prefered node first (will be nodeList[0])
				if (isFactTable){
				  j = 0;
				  iter = theQueue.iterator();
				  while (notFound && iter.hasNext()){
					StripeOrRowgroup elem = iter.next();
					if (elem.nodeList[0] == nodeId){
					  notFound = false;
					  response.getWriter().println(elem.Serialize());//set the result output
					  elem.sequenceNb = sequenceNb; 
					  context.lastRowgroupSents.put(espNb,elem);// set lastRowgroup
					  theQueue.remove(j); // remove from queue	
					}
					j++;
				  }
				}				  
				  //if no match try a data local node
				if (notFound){
				  j=0;
				  iter = theQueue.iterator();
				  while (notFound && iter.hasNext()){
					StripeOrRowgroup elem = iter.next();
					boolean isDataLocal = false;
					for(int k = (isFactTable?1:0); k< elem.nodeList.length; k++){
					   if (elem.nodeList[k] == nodeId){
					     isDataLocal = true;
					     break;
					   }
					}
					if (isDataLocal){
					  notFound = false;
					  response.getWriter().println(elem.Serialize());//set the result output
					  elem.sequenceNb = sequenceNb; 
					  context.lastRowgroupSents.put(espNb,elem);// set lastRowgroup
					  theQueue.remove(j); // remove from queue	
					}
					j++;
				  }//end while				  
				}//endif
				//if no match give up on locality an get first on queue
				if (notFound){
					if (!theQueue.isEmpty()){
					  StripeOrRowgroup elem = theQueue.removeFirst();
				          response.getWriter().println(elem.Serialize());//set the result output
					  elem.sequenceNb = sequenceNb; 
					  context.lastRowgroupSents.put(espNb,elem);// set lastRowgroup
					}else{
					  context.lastRowgroupSents.remove(espNb);
					  response.getWriter().println("QUEUE_EMPTY");
				        }
				}//endif
			}//end synchronized
		    }		    
		    response.setContentType("text/html");
		    response.setStatus(HttpServletResponse.SC_OK);
		    
		}
	}
	public static class StrawScannerEnqueueServlet extends HttpServlet{
		//make sure the list of nodes are deterministic and randomized based on file name and block number.
		//this is to attempt to always allocate the same stripe to the same node, so that we don't thrash disk cache one several node for the same block
		//preferedNode is the node that was used to write the file and is present on all stripes of a given file. Using this node for locality is optimal
		//since in cases where stripes are adjacent to 2 nodes, we are going to still be data local for both blocks.
		private short[] deterministicRandomizedNodeList(short[] nodeList, String fileName, String hdfsBlockNb, short preferedNode){
			Arrays.sort(nodeList);
			CRC32 crc32 = new CRC32();
			crc32.update(fileName.getBytes());
			crc32.update(hdfsBlockNb.getBytes());
			int offs = (int)(crc32.getValue() % (long)nodeList.length);
			short[] result = new short[nodeList.length];
			for (int i=0; i<result.length; i++)
				result[i] = nodeList[(i+offs) % nodeList.length]; 
			for (int i=1; i<result.length;i++)
				if (result[i] == preferedNode){
					result[i] = result[0];
					result[0] = preferedNode;
				}
			return result;
		}
		
		//make sure list of nodes are purely randomized (for low cardinality Dimention tables) where disk cache trashing is not an issue, but hot spot is.
		public static Random random = new Random(System.currentTimeMillis());
		private short[] randomizedNodeList(short[] nodeList){
			int offs = random.nextInt(nodeList.length);
			short[] result = new short[nodeList.length];			
			for (int i=0; i<result.length; i++)
				result[i] = nodeList[(i+offs) % nodeList.length]; 
			return result;
		}

	       
	       private void processStripesStr(String fileName, ArrayList<String[]> stripesStr,LinkedList<StripeOrRowgroup> strawScannerQueue , boolean isFactTable){
			//compute prefered node	
			short preferedNode = -1;		
			String[] preferedNodeCandidates =  stripesStr.get(0)[5].split(" ");			
			boolean foundPreferedNode = false;
			int i = 0;			
			while (!foundPreferedNode && i< preferedNodeCandidates.length){
				boolean stillGoodCandidate = true;
				String matcher = String.format(".* %s .*|%s .*|.* %s",preferedNodeCandidates[i],preferedNodeCandidates[i],preferedNodeCandidates[i]);
				for(int j=1; j<stripesStr.size(); j++){
					if (!stripesStr.get(j)[5].matches(matcher)){
						stillGoodCandidate = false;
						i++;
						break;
					}
				}
				if (stillGoodCandidate){
					preferedNode = Short.parseShort(preferedNodeCandidates[i]);
					foundPreferedNode = true;
				}
			}
				
			for(String[] split : stripesStr){
			    if (split.length == 7){
			      try{
			        String[] s = split[5].split(" ");
			        short[] nodeList = new short[s.length];
			        for(int j=0; j<s.length; j++)
				  nodeList[j] = Short.parseShort(s[j]);
			        StripeOrRowgroup stripeOrRowgroup = new StripeOrRowgroup(
				  fileName,
				  Integer.parseInt(split[0]),
				  Short.parseShort(split[1]),
				  Long.parseLong(split[2]),
				  Long.parseLong(split[3]),
				  split[4].length() == 0 ? Long.parseLong(split[3]) : Long.parseLong(split[4]), //use size if filteredSize is empty
				  isFactTable ? deterministicRandomizedNodeList(nodeList, fileName, split[1],preferedNode) : randomizedNodeList(nodeList),
				  split[6]);
			        strawScannerQueue.add(stripeOrRowgroup);
			    
			      } catch(Exception e){
				LOG.error(String.format("Cannot deserialize: %s \n Exception: %s",split[1], e.toString()));
							      }
			    } else LOG.error(String.format("Cannot deserialize: %s %d",split[1],split.length));
			}//endfor				
	       }
 		@Override
	       protected void doPost( HttpServletRequest request,
		                      HttpServletResponse response ) throws ServletException,
		                                                    IOException
		{
		    boolean isFactTable = false;
            long entryTs = System.currentTimeMillis(); 
		    if (LOG.isDebugEnabled()) LOG.debug(String.format("initStrawScan %s entered",request.getPathInfo()));
		    LinkedList<StripeOrRowgroup> strawScannerQueue = new LinkedList<StripeOrRowgroup>(); 
		    BufferedReader reader = request.getReader();
		    String line;
		    String currentFileName = null;
		    ArrayList<String[]> stripesStr = new ArrayList<String[]>() ;
                    line = reader.readLine();//first line contains isFactTable=true or isFactTable=false
		    if (line.equals("isFactTable=true")) isFactTable = true;
		    while((line = reader.readLine()) != null){
			if (line.startsWith("h")){ //filename CRC32 mark
			  if (currentFileName != null){
			    processStripesStr(currentFileName,stripesStr,strawScannerQueue,isFactTable);
			  }
			  currentFileName = line;
			  stripesStr.clear();			  
			}
			else
			{
			  stripesStr.add(line.split("\\|"));			 
			}		
		    }//end while
		    processStripesStr(currentFileName,stripesStr,strawScannerQueue, isFactTable); // process last batch
		    // sort the list by filtered size so that we can serve bigger stripes first.
		    Collections.sort(strawScannerQueue, new Comparator<StripeOrRowgroup>(){
			public int compare(StripeOrRowgroup obj1, StripeOrRowgroup obj2){
				if (obj2.filteredSize > obj1.filteredSize)
					return 1;
				else if (obj2.filteredSize < obj1.filteredSize)
					return -1;
				else return 0;
			}
		    });
		    immutableQueueIds.put(request.getPathInfo(),strawScannerQueue);	
		    immutableQueueIdCreationTs.put(request.getPathInfo(),System.currentTimeMillis()); //time stamp creation date to allow future memory cleanup
		    response.setContentType("text/html");
		    response.setStatus(HttpServletResponse.SC_CREATED);	
		    //for(StripeOrRowgroup stripeOrRowgroup : strawScannerQueue)
			//System.out.println(stripeOrRowgroup.Serialize());
		    if (LOG.isDebugEnabled())LOG.debug(String.format("initStrawScan %s :timed at %d ms",request.getPathInfo(),System.currentTimeMillis()-entryTs));
		}	
	}
}
