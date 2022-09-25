/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
*/
package org.trafodion.dcs.master;

import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.tmpl.master.MasterStatusTmpl;

/**
 * The servlet responsible for rendering the index page of the
 * master.
 */
public class MasterStatusServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MasterStatusServlet.class);
	private static final long serialVersionUID = 1L;
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException
  {
    DcsMaster master = (DcsMaster) getServletContext().getAttribute(DcsMaster.MASTER);
    assert master != null : "No Master in context!";
    
    List<RunningServer> servers = master.getServerManager().getServersList();
     
    response.setContentType("text/html");
    MasterStatusTmpl tmpl = new MasterStatusTmpl() 
      .setServers(servers);
    if (request.getParameter("filter") != null)
      tmpl.setFilter(request.getParameter("filter"));
    if (request.getParameter("format") != null)
      tmpl.setFormat(request.getParameter("format"));
    tmpl.render(response.getWriter(), master);
  }
}
