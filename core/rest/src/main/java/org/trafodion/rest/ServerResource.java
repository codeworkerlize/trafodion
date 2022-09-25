/**
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

package org.trafodion.rest;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.trafodion.rest.script.ScriptContext;
import org.trafodion.rest.script.ScriptManager;

import com.esgyn.common.LicenseHelper;

public class ServerResource extends ResourceBase {
	private static final Log LOG =
			LogFactory.getLog(ServerResource.class);

	static CacheControl cacheControl;
	static {
		cacheControl = new CacheControl();
		cacheControl.setNoCache(true);
		cacheControl.setNoTransform(false);
	}

	public ServerResource() throws IOException {
		super();
	}

	private String buildRemoteException(String className,String exception,String message) throws IOException {

		try {
			JSONObject jsonRemoteExceptionDetail = new JSONObject();
			jsonRemoteExceptionDetail.put("javaClassName", className);
			jsonRemoteExceptionDetail.put("exception",exception);
			jsonRemoteExceptionDetail.put("message",message);
			JSONObject jsonRemoteException  = new JSONObject();	        
			jsonRemoteException.put("RemoteException",jsonRemoteExceptionDetail);
			if (LOG.isDebugEnabled()) 
				LOG.debug(jsonRemoteException.toString());
			return jsonRemoteException.toString();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
	
	private JSONObject node_sqcheck(String nodeName) throws Exception {
		boolean runLocally = true;
		if(nodeName != null && nodeName.length() > 0) {
			runLocally = isLocalNode(nodeName);
		}
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		if(!runLocally){
		    scriptContext.setCommand("edb_pdsh -w " + nodeName + " node_sqcheck -j");
		}else{
		    scriptContext.setCommand("node_sqcheck -j");
		}
		

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		// node_sqcheck will return:
		// 0 - Fully up and operational
		// 1 - Down
		String state = null;
		int exitCode = scriptContext.getExitCode();
		switch(exitCode) {
		case 0:  
			state = "UP";
			break;
		case 1:   
		case 255:   
			state = "DOWN";
			break;              
		default:
			state = "UNKNOWN";
		}

		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();
		if(exitCode == 0) {
			int downCount = 0;
			try {
				String stdout = scriptContext.getStdOut().toString();
				if(!runLocally){
				   stdout = stdout.substring(stdout.indexOf(":")+1);
				}
				jsonArray = new JSONArray(stdout);
				for(int i=0; i < jsonArray.length(); i++) {
					JSONObject obj = jsonArray.getJSONObject(i);
					if(obj.has("ACTUAL")) {
						int pCount = obj.getInt("ACTUAL");
						if(pCount < 1) {
							downCount ++;
						}
					}
				}
				if(downCount > 0) {
					if(downCount == jsonArray.length())
						state = "DOWN";
					else
						state = "PARTIALLY UP";
				}
				
			} catch (JSONException e) {
				e.printStackTrace();
				LOG.error(e.getMessage());
				throw new Exception("ERROR: Request did not return valid JSON response");
			}
		}
		jsonObject.put("STATE", state);
		jsonObject.put("PROCESSES",jsonArray);

		return jsonObject;
	}	

	private JSONObject sqcheck(String operation) throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("sqcheck -j -c " + operation);

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		// sqcheck will return:
		//-1 - Not up ($?=255)
		// 0 - Fully up and operational
		// 1 - Partially up and operational
		// 2 - Partially up and NOT operational
		String state = null;
		String subState = null;
		int exitCode = scriptContext.getExitCode();
		switch(exitCode) {
		case 0:  
			state = "UP";
			subState = "OPERATIONAL";
			break;
		case 1:   
			state = "PARTIALLY UP";
			subState = "OPERATIONAL";
			break;
		case 2:   
			state = "PARTIALLY UP";
			subState = "NOT OPERATIONAL";
			break;
		case 255:  
			state = "DOWN";
			subState = "NOT OPERATIONAL";
			break;              
		default:
			state = "UNKNOWN";
			subState = "UNKNOWN";
		}

		JSONObject jsonObject = new JSONObject();
		try {
			jsonObject.put("STATE", state);
			jsonObject.put("SUBSTATE", subState);
			String stdout = scriptContext.getStdOut().toString();
			JSONArray jsonArray = new JSONArray(stdout);
			//JSONArray jsonArray = new JSONArray(scriptContext.getStdOut().toString());
			jsonObject.put("PROCESSES",jsonArray);
		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: Request did not return valid JSON response");
		}

		return jsonObject;
	}

	private JSONObject dcscheck() throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("dcscheck");
		scriptContext.setStripStdOut(false);
		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		JSONObject jsonObject = new JSONObject();
		try {
			Scanner scanner = new Scanner(scriptContext.getStdOut().toString());

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if(line.contains(":")){
					String[] nameValue = line.split(":");
					if(nameValue.length > 1){
						jsonObject.put(nameValue[0].trim(), nameValue[1].trim());
					}
				}
			}

			scanner.close();

		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: request did not return valid JSON response");
		}

		return jsonObject;
	}

	private JSONArray pstack(String program) throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("sqpstack " + program);
		scriptContext.setStripStdOut(false);

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		JSONArray json = null;
		try {
			json = new JSONArray();
			StringBuilder sb = new StringBuilder();
			boolean pstack = false;
			Scanner scanner = new Scanner(scriptContext.getStdOut().toString()); 
			while(scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if(line.contains("pstack-ing")) {
					continue;
				} else if (line.contains("pstack") || line.startsWith("--")) {
					if (pstack == true && sb.length() > 0) {
						json.put(new JSONObject().put("PROGRAM", sb.toString()));
						sb.setLength(0);
						if (line.contains("pstack"))
							sb.append(line + "\n");
						pstack = false;
					} else {
						pstack = true;
						if (line.contains("pstack"))
							sb.append(line + "\n");
					}
				} else {
					sb.append(line + "\n");
				}
			}
			scanner.close();
		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: request did not return valid JSON response");
		}

		return json;
	}

	private JSONArray jstack(String program) throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("jstack " + program);
		scriptContext.setStripStdOut(false);

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		JSONArray json = null;
		try {
			json = new JSONArray();
			StringBuilder sb = new StringBuilder();
			Scanner scanner = new Scanner(scriptContext.getStdOut().toString()); 
			while(scanner.hasNextLine()) {
				String line = scanner.nextLine();
				sb.append(line + "\n");
			}
			scanner.close();
			json.put(new JSONObject().put("PROGRAM", sb.toString()));
		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: request did not return valid JSON response");
		}

		return json;
	}

	private JSONObject license(String licenseFileName) throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("decoder -a -f " + licenseFileName);
		scriptContext.setStripStdOut(false);

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		JSONObject json = null;
		try {
			json = new JSONObject();
			Scanner scanner = new Scanner(scriptContext.getStdOut().toString()); 
			while(scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] tokens = line.split(":");
				if(tokens.length > 1){
					json.put(tokens[0].trim(), tokens[1].trim());
				}
			}
			scanner.close();
		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: request did not return valid JSON response");
		}

		return json;
	}

	private JSONArray dcs() throws Exception {

		JSONArray json = new JSONArray();
		try {
			List<RunningServer> servers = servlet.getDcsServersList();
			if(LOG.isDebugEnabled())
				LOG.debug("servers=" + servers);

			for (RunningServer aRunningServer: servers) {
				for (RegisteredServer aRegisteredServer: aRunningServer.getRegistered()) {
					JSONObject obj  = new JSONObject();
					obj.put("HOSTNAME",aRunningServer.getHostname());
					obj.put("START_TIME", aRegisteredServer.getCreateTimeAsDate());
					//obj.put("REGISTERED", aRegisteredServer.getIsRegistered());
					String state = aRegisteredServer.getState();
					obj.put("STATE",state);
					obj.put("NID",aRegisteredServer.getNid());
					obj.put("PID",aRegisteredServer.getPid());
					obj.put("PROCESS_NAME",aRegisteredServer.getProcessName());
					obj.put("IP_ADDRESS",aRegisteredServer.getIpAddress());
					obj.put("PORT",aRegisteredServer.getPort());
                    if(state.equals("CONNECTED") || state.equals("CONNECTING")){
                    	obj.put("USER_NAME", aRegisteredServer.getUserName());
                    	if(LicenseHelper.isMultiTenancyEnabled()) {
                    		obj.put("TENANT_NAME", aRegisteredServer.getTenantName());
                    	}
                    	obj.put("CLIENT_APPL",aRegisteredServer.getClientAppl());
						obj.put("MAPPED_SLA",aRegisteredServer.getSla());
						obj.put("MAPPED_CONNECT_PROFILE",aRegisteredServer.getConnectProfile());
						//obj.put("MAPPED_DISCONNECT_PROFILE",aRegisteredServer.getDisconnectProfile());
						if(state.equals("CONNECTED")) { 
							obj.put("CONNECT_TIME",aRegisteredServer.getConnectTimeAsDate());
							obj.put("CONNECTED_INTERVAL_SEC",aRegisteredServer.getConnectedInterval());
						}else {
							obj.put("CONNECT_TIME","");
							obj.put("CONNECTED_INTERVAL_SEC","");
						}
						obj.put("CLIENT_NAME",aRegisteredServer.getClientName());
						obj.put("CLIENT_IP_ADDRESS",aRegisteredServer.getClientIpAddress());
						obj.put("CLIENT_PORT",aRegisteredServer.getClientPort());
					} else {
                    	obj.put("USER_NAME", "");
                    	if(LicenseHelper.isMultiTenancyEnabled()) {
                    		obj.put("TENANT_NAME", "");
                    	}
                    	obj.put("CLIENT_APPL", "");
						obj.put("MAPPED_SLA","");
						obj.put("MAPPED_CONNECT_PROFILE","");
						//obj.put("MAPPED_DISCONNECT_PROFILE","");
						obj.put("CONNECT_TIME","");
						obj.put("CONNECTED_INTERVAL_SEC","");
						obj.put("CLIENT_NAME", "");
						obj.put("CLIENT_IP_ADDRESS", "");
						obj.put("CLIENT_PORT", "");
					}
					obj.put("INSTANCE",aRunningServer.getInstance());
					json.put(obj);
				}
			}
		} catch (JSONException e) {
			e.printStackTrace();
			LOG.error(e.getMessage());
			throw new Exception("ERROR: request did not return valid JSON response");
		}

		if(LOG.isDebugEnabled())
			LOG.debug("json.length() = " + json.length());

		return json;
	}

	private String nodes() throws Exception {
		ScriptContext scriptContext = new ScriptContext();
		scriptContext.setScriptName(Constants.SYS_SHELL_SCRIPT_NAME);
		scriptContext.setCommand("trafnodestatus -j");

		try {
			ScriptManager.getInstance().runScript(scriptContext);//This will block while script is running
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}

		if(LOG.isDebugEnabled()) {
			StringBuilder sb = new StringBuilder();
			sb.append("exit code [" + scriptContext.getExitCode() + "]");
			if(! scriptContext.getStdOut().toString().isEmpty()) 
				sb.append(", stdout [" + scriptContext.getStdOut().toString() + "]");
			if(! scriptContext.getStdErr().toString().isEmpty())
				sb.append(", stderr [" + scriptContext.getStdErr().toString() + "]");
			LOG.debug(sb.toString());
		}

		return scriptContext.getStdOut().toString();
	}

	@GET
	@Path("/test")
	@Produces({MIMETYPE_JSON})
	public Response test(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			String result = buildRemoteException(
					"org.trafodion.rest.NotFoundException",
					"NotFoundException",
					"This is my exception text");
			return Response.status(Response.Status.NOT_FOUND)
					.type(MIMETYPE_JSON).entity(result)
					.build();

			//ResponseBuilder response = Response.ok(result);
			//response.cacheControl(cacheControl);
			//return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		}
	}    

	@GET
	@Path("/node-health")
	@Produces({MIMETYPE_JSON})
	public Response getNodeHealth(
			final @Context UriInfo uriInfo,
			final @Context Request request,
			@QueryParam("nodeName") String nodeName) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONObject jsonObject = node_sqcheck(nodeName);

			if(jsonObject.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No health check data found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonObject.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}
	
	@GET
	@Produces({MIMETYPE_JSON})
	public Response getAll(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "all");
	}    

	@GET
	@Path("/dtm")
	@Produces({MIMETYPE_JSON})
	public Response getDtm(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "dtm");
	}

	@GET
	@Path("/rms")
	@Produces({MIMETYPE_JSON})
	public Response getRms(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "rms");
	}

	@GET
	@Path("/dcs")
	@Produces({MIMETYPE_JSON})
	public Response getDcs(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "dcs");
	}
	
	@GET
	@Path("/rest")
	@Produces({MIMETYPE_JSON})
	public Response getRestStatus(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "rest");
	}

	@GET
	@Path("/fndn")
	@Produces({MIMETYPE_JSON})
	public Response getFoundationStatus(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "fndn");
	}
	
	@GET
	@Path("/mgblty")
	@Produces({MIMETYPE_JSON})
	public Response getMgbltyStatus(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		return getComponentStatus(uriInfo, request, "mgblty");
	}

	private Response getComponentStatus(
			final @Context UriInfo uriInfo,
			final @Context Request request,
			String componentName) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONObject jsonObject = sqcheck(componentName);

			if(jsonObject.length() == 0) {
				String compName = componentName.equalsIgnoreCase("all") ? "server" : componentName.toLowerCase();
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No "+ compName + " resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonObject.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}
	
	@GET
	@Path("/dcs/summary")
	@Produces({MIMETYPE_JSON})
	public Response getDcsSummary(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONObject jsonObject = dcscheck();

			if(jsonObject.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No dcs resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonObject.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	} 

	@GET
	@Path("/dcs/connections")
	@Produces({MIMETYPE_JSON})
	public Response getConnections(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONArray jsonArray = dcs();
			if(jsonArray.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No dcs connection resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonArray.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}

	@GET
	@Path("/nodes")
	@Produces({MIMETYPE_JSON})
	public Response getNodes(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			String json = nodes();
			if(json.equals("[]")) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No node resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(json);
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}

	@GET
	@Path("/pstack")
	@Produces({MIMETYPE_JSON})
	public Response getPstack(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONArray jsonArray = pstack("");

			if(jsonArray.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No pstack resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonArray.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}

	@GET
	@Path("/pstack/program/{program}")
	@Produces({MIMETYPE_JSON})
	public Response getPstackProgram(
			final @Context UriInfo uriInfo,
			final @Context Request request,
			@PathParam("program") String program) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONArray jsonArray = pstack(program);

			if(jsonArray.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No pstack resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonArray.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}

	@GET
	@Path("/jstack/program/{program}")
	@Produces({MIMETYPE_JSON})
	public Response getJstackProgram(
			final @Context UriInfo uriInfo,
			final @Context Request request,
			@PathParam("program") String program) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());

				MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
				String output = " Query Parameters :\n";
				for (String key : queryParams.keySet()) {
					output += key + " : " + queryParams.getFirst(key) +"\n";
				}
				LOG.debug(output);

				MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
				output = " Path Parameters :\n";
				for (String key : pathParams.keySet()) {
					output += key + " : " + pathParams.getFirst(key) +"\n";
				}
				LOG.debug(output);
			}

			JSONArray jsonArray = jstack(program);

			if(jsonArray.length() == 0) {
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"No pstack resources found");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();
			}

			ResponseBuilder response = Response.ok(jsonArray.toString());
			response.cacheControl(cacheControl);
			return response.build();
		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}

	@GET
	@Path("/license")
	@Produces({MIMETYPE_JSON})
	public Response getProductLicense(
			final @Context UriInfo uriInfo,
			final @Context Request request) {
		try {
			if (LOG.isDebugEnabled()) {
				LOG.debug("GET " + uriInfo.getAbsolutePath());
			}

			String licenseFileName = "/etc/trafodion/esgyndb_license";
			File f = new File(licenseFileName);
			if(f.exists() && !f.isDirectory()) {
				JSONObject jsonObject = license(licenseFileName);

				if(jsonObject.length() == 0) {
					String result = buildRemoteException(
							"org.trafodion.rest.NotFoundException",
							"NotFoundException",
							"No license found");
					return Response.status(Response.Status.NOT_FOUND)
							.type(MIMETYPE_JSON).entity(result)
							.build();
				}

				ResponseBuilder response = Response.ok(jsonObject.toString());
				response.cacheControl(cacheControl);
				return response.build();		
			}else{
				String result = buildRemoteException(
						"org.trafodion.rest.NotFoundException",
						"NotFoundException",
						"Cannot find license file");
				return Response.status(Response.Status.NOT_FOUND)
						.type(MIMETYPE_JSON).entity(result)
						.build();

			}


		} catch (IOException e) {
			e.printStackTrace();
			return Response.status(Response.Status.SERVICE_UNAVAILABLE)
					.type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
					.build();
		} catch(Exception e) {
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
					.type(MIMETYPE_TEXT).entity(e.getMessage() + CRLF)
					.build();
			
		}
	}
	
	private static boolean isLocalNode(String nodeName) {
		try {
			if(nodeName != null) {
		      String[] shortNodeName = nodeName.split("[.]");
		      InetAddress inetAddress = InetAddress.getLocalHost();
		      String localNodeName = inetAddress.getHostName();
			  String[] localShortNodeName = localNodeName.split("[.]");
		      return localShortNodeName[0].equalsIgnoreCase(shortNodeName[0]);
			}
	    } catch (UnknownHostException e) {
	    }
		return false;
	}

    @GET
    @Path("/health")
    @Produces({MIMETYPE_JSON})
    public Response getHealth(
            final @Context UriInfo uriInfo,
            final @Context Request request) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("GET " + uriInfo.getAbsolutePath());

                MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
                String output = " Query Parameters :\n";
                for (String key : queryParams.keySet()) {
                    output += key + " : " + queryParams.getFirst(key) +"\n";
                }
                LOG.debug(output);

                MultivaluedMap<String, String> pathParams = uriInfo.getPathParameters();
                output = " Path Parameters :\n";
                for (String key : pathParams.keySet()) {
                    output += key + " : " + pathParams.getFirst(key) +"\n";
                }
                LOG.debug(output);
            }

            JSONObject jsonObject = sqcheck("dcs");

            if(jsonObject.length() == 0) {
                String result = buildRemoteException(
                        "org.trafodion.rest.NotFoundException",
                        "NotFoundException",
                        "No health resources found");
                return Response.status(Response.Status.NOT_FOUND)
                        .type(MIMETYPE_JSON).entity(result)
                        .build();
            }

            JSONArray jsonArray = jsonObject.getJSONArray("PROCESSES");
            JSONObject subJsonObject;
            int healthPercentage = -1;

            for (int i = 0; i < jsonArray.length() ; i++) {
                subJsonObject = jsonArray.getJSONObject(i);
                if ("MXOSRVR".equals(subJsonObject.getString("PROCESS"))) {
                    int configuredMxoCount = subJsonObject.getInt("CONFIGURED");
                    int actualMxoCount = subJsonObject.getInt("ACTUAL");
                    try {
                        healthPercentage = 100 * actualMxoCount / configuredMxoCount;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            JSONObject returnJsonObj = new JSONObject();
            returnJsonObj.put("HEALTH", String.valueOf(healthPercentage));
            ResponseBuilder response = Response.ok(returnJsonObj);

            response.cacheControl(cacheControl);
            return response.build();

        } catch (Exception e) {
            e.printStackTrace();
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }
}
