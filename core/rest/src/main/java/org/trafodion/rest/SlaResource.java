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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;

import javax.ws.rs.*;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

public class SlaResource extends ResourceBase {
	private static final Log LOG =
		LogFactory.getLog(SlaResource.class);

	static CacheControl cacheControl;
	static {
		cacheControl = new CacheControl();
		cacheControl.setNoCache(true);
		cacheControl.setNoTransform(false);
	}

	public SlaResource() throws IOException {
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
    private JSONObject sla() throws IOException {
        JSONObject json = null;
        try {
            Map<String, LinkedHashMap<String,String>> slas = servlet.getWmsSlasMap();
            json= new JSONObject(slas);
            
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }
        if(LOG.isDebugEnabled())
            LOG.debug("json.length() = " + json.length());

        return json;
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
    @Produces({MIMETYPE_JSON})
    public Response getAll(
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

            JSONObject json = sla();
            
            if(json.length() == 0) {
                String result = buildRemoteException(
                        "org.trafodion.rest.NotFoundException",
                        "NotFoundException",
                        "No server resources found");
                return Response.status(Response.Status.NOT_FOUND)
                        .type(MIMETYPE_JSON).entity(result + CRLF)
                        .build();
            }
 
            ResponseBuilder response = Response.ok(json.toString());
            response.cacheControl(cacheControl);
            return response.build();
        } catch (IOException e) {
            e.printStackTrace();
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    } 
    
    @POST
    @Consumes({MIMETYPE_JSON})
    public Response postSla(String data) {
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("POST " + data);
            String sdata = "";
            Response.Status status = Response.Status.OK; 
            String result = "200 OK.";
            
            
            JSONTokener jsonParser = new JSONTokener(data);
            JSONObject json = (JSONObject)jsonParser.nextValue();

            Iterator<?> keysItr = json.keys();
            while(keysItr.hasNext()) {
                String key = (String)keysItr.next();
                JSONObject value = (JSONObject)json.get(key);
                Iterator<?> itr = value.keys();
                sdata = "";
                boolean lastUpdate = false;
                
                while(itr.hasNext()) {
                    String attrKey = (String)itr.next();
                    String attrValue = value.getString(attrKey);
                    if (sdata.length()!= 0)
                        sdata = sdata + ":";
                    if(attrKey.equals(Constants.LAST_UPDATE)){
                        lastUpdate = true;
                        attrValue = Long.toString(System.currentTimeMillis());
                    }
                    sdata = sdata + attrKey + "=" + attrValue;
                 } 
                if (lastUpdate == false)
                    sdata = sdata + ";" + Constants.LAST_UPDATE + "=" + Long.toString(System.currentTimeMillis());
                result = servlet.postWmsSla(key, sdata);
            }
            if (result.startsWith("200")){
                status = Response.Status.OK;
            }
            else if (result.startsWith("201")){
                status = Response.Status.CREATED;
            }
            else if (result.startsWith("304")){
                status = Response.Status.NOT_MODIFIED;
            }
            else if (result.startsWith("406")){
                status = Response.Status.NOT_ACCEPTABLE;
            }
            result = result.substring(4);
            if (status != Response.Status.OK && status != Response.Status.CREATED)
              return Response.serverError().entity(result).build();

            return Response.status(status).type(MIMETYPE_TEXT).entity(result).build();

        } catch (Exception e) {
            e.printStackTrace();

            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }
    
    private void ToMap(String data)throws Exception {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        JSONTokener jsonParser = new JSONTokener(data);
        
        JSONObject json = (JSONObject)jsonParser.nextValue();
        map = jsonToMap(json);
    }
    
    private Map<String, Object> jsonToMap(JSONObject json) throws Exception {
        Map<String, Object> retMap = new LinkedHashMap<String, Object>();

        if(json != JSONObject.NULL) {
            retMap = toMap(json);
        }
        return retMap;
    }

    private Map<String, Object> toMap(JSONObject object) throws Exception {
        Map<String, Object> map = new LinkedHashMap<String, Object>();

        Iterator<String> keysItr = object.keys();
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);

            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }
        return map;
    }

    private List<Object> toList(JSONArray array) throws Exception {
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if(value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }

            else if(value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }
    @DELETE 
    @Path("/query")
    public Response deleteSla(@QueryParam("delete") String name) {
        try {
            if (LOG.isDebugEnabled())
                LOG.debug("DELETE name :" + name);
            Response.Status status = Response.Status.OK; 
            String result = "200 OK.";
            result = servlet.deleteWmsSla(name);
            
            if (result.startsWith("200")){
                status = Response.Status.OK;
            }
            else if (result.startsWith("201")){
                status = Response.Status.CREATED;
            }
            else if (result.startsWith("304")){
                status = Response.Status.NOT_MODIFIED;
            }
            else if (result.startsWith("406")){
                status = Response.Status.NOT_ACCEPTABLE;
            }
            result = result.substring(4);
            if (status != Response.Status.OK && status != Response.Status.CREATED)
              return Response.serverError().entity(result).build();

            return Response.status(status).type(MIMETYPE_TEXT).entity(result).build();
            
        } catch (Exception e) {
            e.printStackTrace();

            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .type(MIMETYPE_TEXT).entity("Unavailable" + CRLF)
                    .build();
        }
    }

}