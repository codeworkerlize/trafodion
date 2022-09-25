// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import java.io.IOException;


public class LdapHelper{
    private static final Logger _LOG = LoggerFactory.getLogger(LdapHelper.class);
    private static File ldapConfigFile;
    // This is the list of attributes supported in the configuration sections.
    // The attribute names must match (case insensitive) and contain a trailing
    // colon (:), with no space before the colon.
    //
    // There are three subsets of configuration attributes:
    //
    // 1) Attributes used to connect to the OpenLDAP server
    // 2) Attributes used by our code to manage the communication
    // 3) Attributes that are no longer used, but we still accept

    // OpenLDAP configuration attributes
    static final String PREFIX_HOST_NAME =              "ldaphostname:";
    static final String PREFIX_PORT =                   "ldapport:";
    static final String PREFIX_SEARCH_DN =              "ldapsearchdn:";
    static final String PREFIX_SEARCH_PWD =             "ldapsearchpwd:";
    static final String PREFIX_SSL =                    "ldapssl:";
    static final String PREFIX_UNIQUE_IDENTIFIER =      "uniqueidentifier:";

    // Attributes used to manage the communication
    static final String PREFIX_NETWORK_TIMEOUT =        "ldapnetworktimeout:";
    static final String PREFIX_LDAP_TIMEOUT =           "ldaptimeout:";
    static final String PREFIX_TIMELIMIT =              "ldaptimelimit:";
    static final String PREFIX_RETRY_COUNT =            "retrycount:";
    static final String PREFIX_RETRY_DELAY =            "retrydelay:";
    static final String PREFIX_PRESERVE_CONNECTION =    "preserveconnection:";

    // Attributes used to manage the hostnames
    static final String PREFIX_EXCLUDE_BAD_HOSTS =       "ExcludeBadHosts:"; //not used
    static final String PREFIX_LOAD_BALANCE_HOST_NAME =  "LoadBalanceHostName:";
    static final String PREFIX_MAX_EXCLUDE_LIST_SIZE =   "MaxExcludeListSize:"; //not used

    // Attributes used to manage ldap group searches // not yet implemented
    static final String PREFIX_SEARCH_GROUP_BASE =           "ldapsearchgroupbase:";
    static final String PREFIX_SEARCH_GROUP_CUSTOM_FILTER =  "ldapsearchgroupcustomfilter:";
    static final String PREFIX_SEARCH_GROUP_OBJECTCLASS =    "ldapsearchgroupobjectclass:";
    static final String PREFIX_SEARCH_GROUP_MEMBER_ATTR =    "ldapsearchgroupmemberattr:";
    static final String PREFIX_SEARCH_GROUPNAME_ATTR =       "ldapsearchgroupnameattr:";

    // Attributes of Section defaults
    static final String PREFIX_DEFAULT_SECTION =          "defaultsectionname:";
    static final String PREFIX_REFRESH_TIME =             "refreshtime:";
    static final String PREFIX_TLS_CACERTFILENAME =       "tls_cacertfilename:"; //not used. In Java, cert need to be stored in KeyStore
  
    // section delimiter
    static final String PREFIX_SECTION =          "section:";
  
  
    // on 2.6 ldap config file is at $TRAF_HOME/sql/scripts/
    // on 2.7 and up, it was moved to $TRAF_CONF/
    static {
        File file = null;
        if (System.getenv("TRAFAUTH_CONFIGFILE") != null) {
            file = new File(System.getenv("TRAFAUTH_CONFIGFILE"));
            if (file.exists()){
                ldapConfigFile = file;
            }else{
                _LOG.error(String.format("ldap config file TRAFAUTH_CONFIGFILE missing from %s",System.getenv("TRAFAUTH_CONFIGFILE")));
            }
        }else {
            file = new File(System.getenv("TRAF_CONF")+"/.traf_authentication_config");
            if (file.exists()){
                ldapConfigFile = file;
            }else{
                file = new File(System.getenv("TRAF_HOME")+"/sql/scripts/.traf_authentication_config");
                if (file.exists()){
                    ldapConfigFile = file;
                }else{
                    _LOG.error(String.format("ldap config file .traf_authentication_config missing from %s or %s",System.getenv("TRAF_CONF"), System.getenv("TRAF_HOME")+"/sql/scripts"));
                }
            }
        }
        if (System.getProperty("com.sun.jndi.ldap.connect.pool.protocol")== null)
            System.setProperty("com.sun.jndi.ldap.connect.pool.protocol", "plain ssl");// make sure ssl connections can be pooled.
        if (System.getProperty("com.sun.jndi.ldap.connect.pool.timeout")== null)
            System.setProperty("com.sun.jndi.ldap.connect.pool.timeout", "86400000");// timeout an idle connection in the pool if inactive for 24 hours
		
    }//end static

	//make constructor private for singleton design pattern;
    private LdapHelper(){ 
    try {
            //Load EsgynDB cluster information
            readLdapConfig();
        } catch (Exception e) {
            _LOG.error("Failed to load ldap config",e);
        }
        refreshTimer.schedule(new RefreshLdapHelperTask(), this.refreshTime * 1000);
    };


    /**
    * invoke this init function before invoking any of the helper functions over TLS or SSL.
    * Alternatively, you can configure these JVM properties using -D when using the main method. 
    * @param keyStorePath
    * @param keyStorePassword
    * @param trustStorePath
    * @param trustStorePassword
    */
    static public synchronized void  initSSL(String keyStorePath, String keyStorePassword, String trustStorePath, String trustStorePassword) {
        System.setProperty("javax.net.ssl.keyStore",keyStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword",keyStorePassword);
        System.setProperty("javax.net.ssl.trustStore",trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword",trustStorePassword);	 
    }

    static LdapHelper singletonLdapHelper = null;
	
    static Timer refreshTimer = new Timer();

    private class RefreshLdapHelperTask extends TimerTask{
        public void run() {
            LdapHelper ldapHelper = new LdapHelper();
            if (ldapHelper.configSectionReadSuccess) {
                singletonLdapHelper = ldapHelper;
				 _LOG.info("refresh ldap config success");
            }		 
            else{
            	_LOG.error("failed to refresh ldap config");
            }
       }
    }


    static public synchronized LdapHelper getInstance(){
        if (singletonLdapHelper == null) singletonLdapHelper = new LdapHelper();
        return singletonLdapHelper;
    }

    long refreshTime = 1800; //30 minutes
    String TLS_CACERTFilename;
    String defaultSection = "local";
    short defaultSectionNumber = 0;
    boolean configSectionReadSuccess = false;
    HashMap<String,LdapHostConfig> ldapHostConfigs = new HashMap<String,LdapHostConfig>();
    List<LdapHostConfig> ldapHostConfigsByNumber =  new ArrayList<LdapHostConfig>();
    LdapHostConfig defaultLdapHostConfig = null;


    // LdapHostConfig object initialized with default values. See ldapconfigfile.h for consistency of default value between cpp and java ldap code
   private class LdapHostConfig  {
      List<String>   hostNames = new ArrayList<String>();
      HashMap<String,String>  loadBalancerHostNames = new HashMap<String,String>();
      boolean        excludeBadHosts = true;
      long           maxExcludeListSize = 3;
      long           portNumber = 389;
      String         searchDN = "";
      String         searchPwd = "";
      short           SSL_Level = 0;
      List<String>   uniqueIdentifier = new ArrayList<String>();
      String         searchGroupBase= "";
      String         searchGroupCustomFilter= "";
      String         searchGroupObjectClass = "groupOfNames";
      String         searchGroupMemberAttr = "member";
      String         searchGroupNameAttr = "cn";
      long           networkTimeout = 30;
      long           ldapTimeout = 30;
      long           timeLimit = 30;
      short          retryCount = 5;
      long           retryDelay= 2;
      boolean        preserveConnection = false;

    private String buildLdapHostsString() {
        StringBuilder sb = new StringBuilder();
        // put load balancer hosts first
        for (String host : hostNames) {
            if (loadBalancerHostNames.containsKey(host))
                sb.append(String.format("%s://%s:%d ", SSL_Level == 1 ? "ldaps":"ldap",host,portNumber));
        }
        // put non load balancer hosts second
        for (String host : hostNames) {
            if (!loadBalancerHostNames.containsKey(host))
                sb.append(String.format("%s://%s:%d ", SSL_Level == 1 ? "ldaps":"ldap",host,portNumber));
        }
        return sb.toString().trim();
    }
 
    public Properties buildProperties(boolean forSearch) {
      Properties props = new Properties();
      props.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      props.put(Context.PROVIDER_URL, buildLdapHostsString());
      if (forSearch) {
          if (!searchDN.isEmpty() && SSL_Level != 2) { // don't set credential if TLS is enabled
              props.put(Context.SECURITY_PRINCIPAL, searchDN);
              props.put(Context.SECURITY_CREDENTIALS, searchPwd );  
          }
      }
      if (preserveConnection && SSL_Level != 2) props.put("com.sun.jndi.ldap.connect.pool", "true"); // don't enable connection pool with TLS. Not supported by jndi implementation.  
      props.put("com.sun.jndi.ldap.connect.timeout",Long.toString(networkTimeout*1000));
      if (SSL_Level == 1) {
          props.put(Context.SECURITY_PROTOCOL, "ssl");
      }
      return props;	   
    }
   }//end LdapHostConfig private class
  



    private void readLdapConfig(){
      BufferedReader br=null;;
      try{
        br = new BufferedReader(new FileReader(ldapConfigFile));
        String st;
        Boolean isDefaultSection = null; 
        LdapHostConfig wLdapHostConfig = null;
        while((st=br.readLine()) != null){
            String trimedLine = st.trim();
            if (trimedLine.startsWith("#")) continue; //skip comment lines
            int columnIndex = trimedLine.indexOf(':');
            if (columnIndex <1) continue; //skip invalid line that does not contain a :
            String leftToken = trimedLine.substring(0,columnIndex+1).toLowerCase();
            String rightToken = trimedLine.substring(columnIndex+1).trim();
            if (isDefaultSection == null) { // not yet matched the first SECTION: 
                if (leftToken.equals(PREFIX_SECTION)) {
                    if (rightToken.equalsIgnoreCase("defaults")) {
                        isDefaultSection = true;
                    } else {
                        isDefaultSection = false;
                        wLdapHostConfig = new LdapHostConfig();
                        ldapHostConfigs.put(rightToken.toLowerCase(), wLdapHostConfig);
                        ldapHostConfigsByNumber.add(wLdapHostConfig);
                    }
                }else {
                    _LOG.error("Failed to read ldap config: expecting keyword:"+ PREFIX_SECTION);
                }
           }else if (isDefaultSection == true) {// we are in default section
               switch(leftToken) {
                 case PREFIX_DEFAULT_SECTION:
                  this.defaultSection = rightToken;
                  break;
                 case PREFIX_REFRESH_TIME:
                  try {
                   this.refreshTime = Long.parseLong(rightToken);
                  }catch(NumberFormatException e) {
                    _LOG.error("ldap config refreshTime invalid:"+rightToken,e);
              	  }
              	  break;
              	case PREFIX_TLS_CACERTFILENAME:
             	  this.TLS_CACERTFilename = rightToken;
             	  break;
             	case PREFIX_SECTION:
               	  if (rightToken.equalsIgnoreCase("defaults")) {
                 	  isDefaultSection = true;
                  } else {
                      isDefaultSection = false;
                      wLdapHostConfig = new LdapHostConfig();
                      ldapHostConfigs.put(rightToken.toLowerCase(), wLdapHostConfig);
                      ldapHostConfigsByNumber.add(wLdapHostConfig);
                      if (this.defaultSection.equals(rightToken.toLowerCase())) {
                          this.defaultSectionNumber = (short)(ldapHostConfigsByNumber.size()-1);
                      }
                  }
             	  break;
            	default:
               	  _LOG.error("ldap config unknown token:"+leftToken +" "+rightToken);
               	  break;
               }
            }else {//we are in non default section
               switch(leftToken) {
                 case PREFIX_HOST_NAME:
                     wLdapHostConfig.hostNames.add(rightToken.toLowerCase());
                     break;
                 case PREFIX_PORT:
                     try {
                         wLdapHostConfig.portNumber = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config port number invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_SEARCH_DN:
                     wLdapHostConfig.searchDN = rightToken;
                     break;
                 case PREFIX_SEARCH_PWD:
                     wLdapHostConfig.searchPwd = rightToken;
                     break;
                 case PREFIX_SSL:
                     try {
                         short ssl = Short.parseShort(rightToken);
                         if (ssl>=0 && ssl<=2) wLdapHostConfig.SSL_Level = ssl;
                         else _LOG.error("ldap config ssl level invalid. Must be 0,1 or 2 but is:"+rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config ssl level invalid. Must be 0,1 or 2 but is:"+rightToken,e);
                     }
                     break;
                 case PREFIX_UNIQUE_IDENTIFIER:
                     wLdapHostConfig.uniqueIdentifier.add(rightToken);
                     break;
                 case PREFIX_NETWORK_TIMEOUT:
                     try {
                         wLdapHostConfig.networkTimeout = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config network timeout invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_LDAP_TIMEOUT:
                     try {
                         wLdapHostConfig.ldapTimeout = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config ldap timeout invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_TIMELIMIT:
                     try {
                         wLdapHostConfig.timeLimit = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config time limit invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_RETRY_COUNT:
                     try {
                         wLdapHostConfig.retryCount = Short.parseShort(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config ldap retry count invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_RETRY_DELAY:
                     try {
                         wLdapHostConfig.retryDelay = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config ldap retry delay invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_PRESERVE_CONNECTION:
                     if (rightToken.equalsIgnoreCase("yes"))
                         wLdapHostConfig.preserveConnection = true;
                     else if (rightToken.equalsIgnoreCase("no"))
                         wLdapHostConfig.preserveConnection = false;
                     else
                         _LOG.error("ldap config preserve connection invalid:"+rightToken);
                     break;
                 case PREFIX_EXCLUDE_BAD_HOSTS:
                     if (rightToken.equalsIgnoreCase("yes"))
                         wLdapHostConfig.excludeBadHosts = true;
                     else if (rightToken.equalsIgnoreCase("no"))
                         wLdapHostConfig.excludeBadHosts = false;
                     else
                         _LOG.error("ldap config exclude bad host invalid:"+rightToken);
                     break;
                 case PREFIX_LOAD_BALANCE_HOST_NAME:
                     wLdapHostConfig.loadBalancerHostNames.put(rightToken.toLowerCase(), "");				
                     break;
                 case PREFIX_MAX_EXCLUDE_LIST_SIZE:
                     try {
                         wLdapHostConfig.maxExcludeListSize = Long.parseLong(rightToken);
                     }catch(NumberFormatException e) {
                         _LOG.error("ldap config ldap max exclude list size invalid:"+rightToken,e);
                     }
                     break;
                 case PREFIX_SEARCH_GROUP_BASE:
                     wLdapHostConfig.searchGroupBase = rightToken;
                     break;
                 case PREFIX_SEARCH_GROUP_CUSTOM_FILTER:
                     wLdapHostConfig.searchGroupCustomFilter = rightToken;
                     break;
                 case PREFIX_SEARCH_GROUP_OBJECTCLASS:
                     wLdapHostConfig.searchGroupObjectClass = rightToken;
                     break;
                 case PREFIX_SEARCH_GROUP_MEMBER_ATTR:
                     wLdapHostConfig.searchGroupMemberAttr = rightToken;
                     break;
                 case PREFIX_SEARCH_GROUPNAME_ATTR:
                     wLdapHostConfig.searchGroupNameAttr = rightToken;
                     break;
                 case PREFIX_SECTION:
                     if (rightToken.equalsIgnoreCase("defaults")) {
                         isDefaultSection = true;
                     } else {
                         isDefaultSection = false;
                         wLdapHostConfig = new LdapHostConfig();
                         ldapHostConfigs.put(rightToken.toLowerCase(), wLdapHostConfig);
                         ldapHostConfigsByNumber.add(wLdapHostConfig);
                         if (this.defaultSection.equals(rightToken.toLowerCase())) {
                             this.defaultSectionNumber = (short)(ldapHostConfigsByNumber.size()-1);
                         }
                     }
                     break;
                 default:
                     _LOG.error("ldap config unknown token:"+leftToken +" "+rightToken);
                     break;
               }
           }
        }
        this.defaultLdapHostConfig = this.ldapHostConfigs.get(this.defaultSection);
        this.configSectionReadSuccess = true;

     }catch(IOException e){
         _LOG.error("Failed to read ldap config",e);
     }finally {
         if (br != null) try {br.close();} catch (IOException e) {}
     }
    }
 
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DefaultSectionName: ").append(this.defaultSection).append('\n')
          .append("RefreshTime: ").append(this.refreshTime).append('\n')
          .append("TLS_CACERTFilename: ").append(this.TLS_CACERTFilename).append('\n').append('\n');
        for (Entry<String,LdapHostConfig> lhc : this.ldapHostConfigs.entrySet()) {
            sb.append("section: ").append(lhc.getKey()).append('\n');
            for (Entry<String,String> lb : lhc.getValue().loadBalancerHostNames.entrySet())
                sb.append("LoadBalanceHostName: ").append(lb.getKey()).append('\n');
            for (String lh : lhc.getValue().hostNames)
                sb.append("LDAPHostName: ").append(lh).append('\n');
            sb.append("LDAPPort: ").append(lhc.getValue().portNumber).append('\n');
            for (String ui : lhc.getValue().uniqueIdentifier)
                sb.append("uniqueIdentifier: ").append(ui).append('\n');
            sb.append("LDAPSearchDN: ").append(lhc.getValue().searchDN).append('\n');
            sb.append("LDAPSearchPwd: ").append(lhc.getValue().searchPwd.equals("")?"":"***").append('\n');
            sb.append("LDAPSSL: ").append(lhc.getValue().SSL_Level).append('\n');
            sb.append("LDAPNetworkTimeout: ").append(lhc.getValue().networkTimeout).append('\n');
            sb.append("LDAPTimeout: ").append(lhc.getValue().ldapTimeout).append('\n');
            sb.append("LDAPTimeLimit: ").append(lhc.getValue().timeLimit).append('\n');
            sb.append("RetryCount: ").append(lhc.getValue().retryCount).append('\n');
            sb.append("RetryDelay: ").append(lhc.getValue().retryDelay).append('\n');
            sb.append("PreserveConnection: ").append(lhc.getValue().preserveConnection).append('\n');
            sb.append("ExcludeBadHosts: ").append(lhc.getValue().excludeBadHosts).append('\n');
            sb.append("MaxExcludeListsize: ").append(lhc.getValue().maxExcludeListSize).append('\n');
            sb.append("LDAPSearchGroupBase: ").append(lhc.getValue().searchGroupBase).append('\n');
            sb.append("LDAPSearchGroupObjectClass: ").append(lhc.getValue().searchGroupObjectClass).append('\n');
            sb.append("LDAPSearchGroupMemberAttr: ").append(lhc.getValue().searchGroupMemberAttr).append('\n');
            sb.append("LDAPSearchGroupCustomFilter: ").append(lhc.getValue().searchGroupCustomFilter).append('\n');
            sb.append("LDAPSearchGroupNameAttr: ").append(lhc.getValue().searchGroupNameAttr).append('\n');
         }
         return sb.toString();
    }

    public class LdapHelperException extends Exception{

        public LdapHelperException(String format) {
            super(format);
            _LOG.error("LdapHelperException: "+ format);
        }
        private static final long serialVersionUID = 1L;
    }

    //return structure for IsRegisteredUser
    public enum IsRegisteredUserStatus {AUTH_OK,AUTH_REJECTED,AUTH_FAILED};
    public class IsRegisteredUserReturn{
        public IsRegisteredUserReturn(String uniqueId, IsRegisteredUserStatus status, LdapHostConfig ldapHostConfig, long connectionTime, long searchTime) {
            super();
            this.uniqueId = uniqueId;
            this.status = status;
            this.ldapHostConfig = ldapHostConfig;
            this.connectionTime = connectionTime;
            this.searchTime = searchTime;
            if (this.status == IsRegisteredUserStatus.AUTH_FAILED)
                _LOG.error(String.format("LDAP search user failed for user: %s",this.uniqueId));
            else
                _LOG.info(String.format("LDAP search user: %s %s :Connection:%d search:%d",this.uniqueId,this.status,this.connectionTime,this.searchTime));
        }
        public IsRegisteredUserStatus getStatus() {
            return this.status;
        }
        String uniqueId;
        IsRegisteredUserStatus status;
        LdapHostConfig ldapHostConfig;
        long connectionTime;
        long searchTime;
    }

    private IsRegisteredUserReturn isRegisteredUser(String userName, LdapHostConfig lhc, int retryNb) {
        Properties props = lhc.buildProperties(true);
        InitialLdapContext context=null;
        NamingEnumeration<SearchResult> results=null;
        StartTlsResponse tls = null;
        try {
            long startTime = System.nanoTime();
            context = new InitialLdapContext(props,null);
            if (lhc.SSL_Level == 2) {//if TLS
                tls = (StartTlsResponse) (context).extendedOperation(new StartTlsRequest());
                try {
                    tls.negotiate();
                    } catch (IOException e) {
                        _LOG.error("Failed to negociate TLS start:", e);
                        return new IsRegisteredUserReturn(null,IsRegisteredUserStatus.AUTH_FAILED,lhc,0,0);
                    }
                if (!lhc.searchDN.isEmpty()) {
                    context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
                    context.addToEnvironment(Context.SECURITY_PRINCIPAL,lhc.searchDN );
                    context.addToEnvironment(Context.SECURITY_CREDENTIALS, lhc.searchPwd);
                }
            }//end if TLS
            long stoptTime = System.nanoTime();
            long connectionTime = (stoptTime - startTime)/1000;
            SearchControls ctrls = new SearchControls();
            ctrls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            long searchTime = 0;
            for (String uniqueId : lhc.uniqueIdentifier) {
                try {
                    startTime = System.nanoTime();
                    results = context.search(uniqueId.replaceFirst("=","="+userName),"(objectClass=user)", ctrls);
                    stoptTime = System.nanoTime();
                    searchTime += (stoptTime - startTime)/1000;
                    return  new IsRegisteredUserReturn(uniqueId.replaceFirst("=","="+userName),IsRegisteredUserStatus.AUTH_OK, lhc,connectionTime,searchTime);
                }catch(NameNotFoundException e){
                    continue;
                }
            }//end for
            return new IsRegisteredUserReturn(null,IsRegisteredUserStatus.AUTH_REJECTED,lhc,connectionTime,searchTime);
        }
        catch(NamingException e) {
        } finally {
            try { if (context != null) context.close(); } catch(Exception ex) { }
            try { if (results != null) results.close(); } catch(Exception ex) { }
            try { if (tls != null) tls.close();}  catch(Exception ex) { }
        }       
        return new IsRegisteredUserReturn(null,IsRegisteredUserStatus.AUTH_FAILED,lhc,0,0);
    }

    public  IsRegisteredUserReturn isRegisteredUser(String userName,   short configNumber, String configName) throws LdapHelperException{
        LdapHostConfig lhc = null;;
        if (configName != null) {
            lhc = this.ldapHostConfigs.get(configName.toLowerCase());
            if (lhc == null) {
                throw new LdapHelperException(String.format("Ldap config for configName %s not found", configName));
            }
        }else if (configNumber >= 0) {
            if (configNumber >= this.ldapHostConfigsByNumber.size()) {
                throw new LdapHelperException(String.format("Ldap config for configNumber %d not found", configNumber));
            }else {
                lhc = this.ldapHostConfigsByNumber.get(configNumber);
                if (lhc == null) {
                    throw new LdapHelperException(String.format("Ldap config for configName %s not found", configName));
                }
            }		 
        }else if (configNumber == -1) {
            lhc = this.defaultLdapHostConfig;	
            if (lhc == null) {
                throw new LdapHelperException(String.format("Ldap default config is null"));
            }
        }
        if (configName == null && configNumber == -2) {// try all configs
        	IsRegisteredUserReturn result = null;
            for (int retry=0;retry<this.defaultLdapHostConfig.retryCount;retry++) {
                for (int confNum = 0; confNum< this.ldapHostConfigsByNumber.size(); confNum++) {
                    result = isRegisteredUser(userName,  this.ldapHostConfigsByNumber.get(confNum), retry);
                    if (result.status == IsRegisteredUserStatus.AUTH_OK) {
                        break;
                    }
                }
                if (result.status == IsRegisteredUserStatus.AUTH_OK || result.status == IsRegisteredUserStatus.AUTH_REJECTED)
                    break;
                try { Thread.sleep(this.defaultLdapHostConfig.retryDelay * 1000);} catch (InterruptedException e) {}
            }
            return result;
        }else{
            for (int retry=0;retry<lhc.retryCount;retry++) {
                IsRegisteredUserReturn result = isRegisteredUser(userName,  lhc, retry);
                if (result.status == IsRegisteredUserStatus.AUTH_FAILED) {
                    _LOG.error(String.format("isRegisteredUser failed for %s, retry nb: %d"),userName,retry);
                    try { Thread.sleep(lhc.retryDelay * 1000);} catch (InterruptedException e) {}
                    continue;
                }else {
                    return result;
                }
            }
            return new IsRegisteredUserReturn(null,IsRegisteredUserStatus.AUTH_FAILED,lhc,0,0);
        }	
    }

    //return structure for AuthenticateUser and AuthenticateUserByUserName
    public enum AuthenticateUserStatus {AUTH_OK, AUTH_REJECTED, AUTH_UNKNOWN_USER, AUTH_FAILED};
    public class AuthenticateUserReturn{
        public AuthenticateUserReturn(AuthenticateUserStatus status, String uniqueId,  LdapHostConfig ldapHostConfig, long connectionTime, long searchTime, long authenticateTime) {
            super();
            this.uniqueId = uniqueId;
            this.status = status;
            this.ldapHostConfig = ldapHostConfig;
            this.connectionTime = connectionTime;
            this.searchTime = searchTime;
            this.authenticateTime = authenticateTime;
            if (this.status == AuthenticateUserStatus.AUTH_FAILED)
                _LOG.error(String.format("LDAP authenticate user failed for user: %s",this.uniqueId));
            else
                _LOG.info(String.format("LDAP search user: %s %s :Connection:%d search:%d authenticate:%d",this.uniqueId,this.status,this.connectionTime,this.searchTime,this.authenticateTime));
        }
        public AuthenticateUserStatus getStatus() {
            return this.status;
        }
        String uniqueId;
        AuthenticateUserStatus status;
        LdapHostConfig ldapHostConfig;
        long connectionTime;
        long searchTime;
        long authenticateTime;
    }
	
    private  AuthenticateUserReturn authenticateUser(String uniqueId, String password,  LdapHostConfig lhc, int retryNb) {
        if (password == null || password.isEmpty())
            return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_REJECTED,uniqueId,lhc,0,0,0);

        Properties props = lhc.buildProperties(false);
        if (lhc.SSL_Level != 2) { // not TLS
            props.put(Context.SECURITY_PRINCIPAL, uniqueId);
            props.put(Context.SECURITY_CREDENTIALS, password );  
        }
        InitialLdapContext context=null;
        NamingEnumeration<SearchResult> results=null;
        StartTlsResponse tls = null;
        long connectionTime = 0;
        long authenticateTime = 0;
        try {
            long startTime = System.nanoTime();
            context = new InitialLdapContext(props,null);
            if (lhc.SSL_Level == 2) {//if TLS
                tls = (StartTlsResponse) (context).extendedOperation(new StartTlsRequest());
                try {
                    tls.negotiate();
                } catch (IOException e) {
                    _LOG.error("LDAP authenticateUser failed to negociate TLS start :",e);
                    return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,uniqueId,lhc,0,0,0);
                }
                context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
                context.addToEnvironment(Context.SECURITY_PRINCIPAL,uniqueId );
                context.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
            }//end if TLS 
            long stopTime = System.nanoTime();
            connectionTime = (stopTime - startTime)/1000;
            SearchControls ctrls = new SearchControls();
            ctrls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
	
            try {
                startTime = System.nanoTime();
                results = context.search(uniqueId,"(objectClass=user)", ctrls);
                stopTime = System.nanoTime();
                authenticateTime = (stopTime - startTime)/1000;
                return  new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_OK,uniqueId,lhc,connectionTime,0,authenticateTime);
            }catch(NameNotFoundException e){}
            return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_REJECTED,uniqueId,lhc,connectionTime,0,authenticateTime);
        }
        catch(NamingException e) {
        } finally {
            try { if (context != null)context.close(); } catch(Exception ex) { }
            try { if (results != null) results.close(); } catch(Exception ex) { }
            try { if (tls != null) tls.close();}  catch(Exception ex) { }
        }       
        return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,uniqueId,lhc,connectionTime,0,authenticateTime);
    }

    public  AuthenticateUserReturn authenticateUser(String uniqueId, String password, LdapHostConfig lhc) {
        for (int retry=0;retry<lhc.retryCount;retry++) {
            AuthenticateUserReturn result = authenticateUser(uniqueId, password,  lhc, retry);
            if (result.status == AuthenticateUserStatus.AUTH_FAILED) {
            	_LOG.error(String.format("authenticateUser failed for %s, retry nb: %d",uniqueId,retry));
                try { Thread.sleep(lhc.retryDelay * 1000);} catch (InterruptedException e) {}
                continue;
            }else {
                return result;
            }		 
        }
        return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,uniqueId,lhc,0,0,0);
    }
	
    private  AuthenticateUserReturn authenticateUserByUserName(String userName, String password,  LdapHostConfig lhc, int retryNb) {
        if (password == null || password.isEmpty())
            return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_REJECTED,userName,lhc,0,0,0);
        /// search user
        Properties props = lhc.buildProperties(true);
        InitialLdapContext context=null;
        NamingEnumeration<SearchResult> results=null;
        StartTlsResponse tls = null;
        String userId = null;
        try {
            long startTime = System.nanoTime();
            context = new InitialLdapContext(props,null);
            if (lhc.SSL_Level == 2) {//if TLS
                tls = (StartTlsResponse) (context).extendedOperation(new StartTlsRequest());
                try {
                    tls.negotiate();
                } catch (IOException e) {
                	_LOG.error("LDAP authenticateUserByUserName failed to negotiate TLS start :",e);
                    return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,userName,lhc,0,0,0);
                }
                if (!lhc.searchDN.isEmpty()) {
                    context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
                    context.addToEnvironment(Context.SECURITY_PRINCIPAL,lhc.searchDN );
                    context.addToEnvironment(Context.SECURITY_CREDENTIALS, lhc.searchPwd);
                }
            }//end if TLS
            long stoptTime = System.nanoTime();
            long connectionTime = (stoptTime - startTime)/1000;
            //_LOG
            SearchControls ctrls = new SearchControls();
            ctrls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            long searchTime = 0;
            for (String uniqueId : lhc.uniqueIdentifier) {
                try {
                    startTime = System.nanoTime();
                    results = context.search(uniqueId.replaceFirst("=","="+userName),"(objectClass=user)", ctrls);
                    stoptTime = System.nanoTime();
                    searchTime += (stoptTime - startTime)/1000;
                    results.close();
                    userId = uniqueId.replaceFirst("=","="+userName);
                }catch(NameNotFoundException e){
                    continue;
                }
            }//end for
            if (userId==null)
                return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_UNKNOWN_USER,userName,lhc,connectionTime,searchTime,0);
            //authenticate           
            long stopTime = 0;
            startTime = System.nanoTime();
            context.addToEnvironment(Context.SECURITY_AUTHENTICATION, "simple");
            context.addToEnvironment(Context.SECURITY_PRINCIPAL,userId );
            context.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
            long authenticateTime = 0;
            try {      		     
                results = context.search(userId,"(objectClass=user)", ctrls);
                stopTime = System.nanoTime();
                authenticateTime = (stopTime - startTime)/1000;
                return  new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_OK,userName,lhc,connectionTime,searchTime,authenticateTime);
            }catch(NameNotFoundException e){
                stopTime = System.nanoTime();
                authenticateTime = (stopTime - startTime)/1000;
            }
            return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_REJECTED,userName,lhc,connectionTime,searchTime,authenticateTime);
        }
        catch(NamingException e) {      	   
        } finally {
            try { if (context != null) context.close(); } catch(Exception ex) { }
            try { if (results != null) results.close(); } catch(Exception ex) { }
            try { if (tls != null) tls.close();}  catch(Exception ex) { }
        }       
        return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,userName,lhc,0,0,0);
    }

    public AuthenticateUserReturn authenticateUserByUserName(String userName, String password) throws LdapHelperException {
        return authenticateUserByUserName(userName, password, (short)-2, null );
    }
    
    public AuthenticateUserReturn authenticateUserByUserName(String userName, String password, short configNumber, String configName ) throws LdapHelperException {
        LdapHostConfig lhc = null;
        if (configName != null) {
            lhc = this.ldapHostConfigs.get(configName.toLowerCase());
            if (lhc == null) {
                throw new LdapHelperException(String.format("Ldap config for configName %s not found", configName));
            }
        }else if (configNumber >= 0) {
            if (configNumber >= this.ldapHostConfigsByNumber.size()) {
                throw new LdapHelperException(String.format("Ldap config for configNumber %d not found", configNumber));
            }else {
                lhc = this.ldapHostConfigsByNumber.get(configNumber);
                if (lhc == null) {
                    throw new LdapHelperException(String.format("Ldap config for configName %s not found", configName));
                }
            }		 
        }else if (configNumber == -1) {
            lhc = this.defaultLdapHostConfig;	
            if (lhc == null) {
                throw new LdapHelperException(String.format("Ldap default config is null"));
            }
        }
        if (configName == null && configNumber == -2) {// try all configs
            AuthenticateUserReturn result = null;
            if (defaultLdapHostConfig == null) {
                throw new LdapHelperException(String.format("Ldap default config is null"));
            }
            for (int retry=0;retry<this.defaultLdapHostConfig.retryCount;retry++) {
                for (int confNum = 0; confNum< this.ldapHostConfigsByNumber.size(); confNum++) {
                    result = authenticateUserByUserName(userName, password, this.ldapHostConfigsByNumber.get(confNum), retry);
                    if (result.status == AuthenticateUserStatus.AUTH_OK) {
                        break;
                    }
                }
                if (result.status == AuthenticateUserStatus.AUTH_OK || result.status == AuthenticateUserStatus.AUTH_REJECTED)
                    break;
                try { Thread.sleep(this.defaultLdapHostConfig.retryDelay * 1000);} catch (InterruptedException e) {}
            }
            return result;
        }else {
            for (int retry=0;retry<lhc.retryCount;retry++) {
                AuthenticateUserReturn result = authenticateUserByUserName(userName, password,  lhc, retry);
                if (result.status == AuthenticateUserStatus.AUTH_FAILED) {
                	_LOG.error(String.format("authenticateUserByUserName failed for %s, retry nb: %d",userName,retry));
                    try { Thread.sleep(lhc.retryDelay * 1000);} catch (InterruptedException e) {}
                    continue;
                }else {
                    return result;
                }
            }
            return new AuthenticateUserReturn(AuthenticateUserStatus.AUTH_FAILED,userName,lhc,0,0,0);
        }
    }

    public static void printUsage() {
        StringBuilder sb =  new StringBuilder();
        sb.append("Usage:java -Djavax.net.ssl.keyStore=/opt/trafodion/sqcert/server.keystore -Djavax.net.ssl.keyStorePassword=password -Djavax.net.ssl.trustStore=/opt/trafodion/sqcert/server.keystore -Djavax.net.ssl.trustStorePassword=password com.esgyn.common.LdapHelper[option]...\n")
          .append("option ::= --help|-h            display usage information\n")
          .append("           --username=<LDAP-username>\n")
          .append("           --password=[<password>] (if no password provided, will do a search only)\n")
          .append("  or       --groupname=<LDAP-groupname> not supported yet\n")
          .append("           --confignumber=<config-section-number> (-1 for default section, -2 for all sections)\n")
          .append("           --configname=<config-section-name>\n (configname is higher priority than configNumber)" )
          .append("           --verbose            Display non-zero retry counts and LDAP errors\n" );
        System.out.print(sb.toString());
    }

    public static void main(String[] args) throws LdapHelperException{
        for (String arg: args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            }		
        }
        String userName=null;;
        String password=null;;
        String groupName=null;
        short configNumber = (short)-1;
        String configName = null;
        boolean verbose = false;

        for (String arg:args) {
            if (arg.equals("--verbose")) {
                verbose = true;
                continue;
            }
            String left = arg.substring(0, arg.indexOf('='));
            String right = arg.substring(arg.indexOf('=')+1).trim();
            switch(left) {
              case "--username":	
                userName= right;
                break;
              case "--password":
                password=right;
                break;
              case "--groupname":
                groupName = right;
                System.out.println("groupName not yet supported : "+groupName);
                System.exit(0);
                break;
              case "--confignumber":
                try {
                    configNumber = Short.parseShort(right);
                }catch (Exception e){
                    System.out.println("Cannot parse configNumber to a short : "+right);
                    System.exit(0);
                }
                break;
              case "--configname":
                configName = right;
                break;
              default:
                System.out.println(String.format("Invalid parameter: %s", arg));
                printUsage();
                System.exit(0);
                break;	
            }
        }

        LdapHelper ldapHelper = LdapHelper.getInstance();
        if (verbose) System.out.println(ldapHelper.toString());
        LdapHelper.IsRegisteredUserReturn result;
        result = ldapHelper.isRegisteredUser(userName,configNumber,configName);
        if (result.status==IsRegisteredUserStatus.AUTH_OK ) {
            System.out.println(String.format("Found user: %s",result.uniqueId));
            if (verbose) System.out.println(String.format("Search Connection time: %d, seach time: %d",result.connectionTime,result.searchTime));
        }
        else if (result.status == IsRegisteredUserStatus.AUTH_REJECTED) {
            System.out.println("User does not exist");
            if (verbose) System.out.println(String.format("Search Connection time: %d, seach time: %d",result.connectionTime,result.searchTime));
            System.exit(0);
        } else {
            System.out.println("Failed to search user");
            if (verbose) System.out.println(String.format("Search Connection time: %d, seach time: %d",result.connectionTime,result.searchTime));
            System.exit(0); 		
        }
        if (password != null && !password.isEmpty()) {
            System.out.println("Authenticate after search:");	
            AuthenticateUserReturn res = ldapHelper.authenticateUser(result.uniqueId, password,result.ldapHostConfig);
            System.out.println(res.status);
            if (verbose) System.out.println(String.format("Authenticate Connection time: %d, authenticate time: %d",res.connectionTime,res.authenticateTime));
            System.out.println("Search and Authenticate in one call:");
            res= ldapHelper.authenticateUserByUserName(userName, password,configNumber,configName);
            System.out.println(res.status);
            if (verbose) System.out.println(String.format("Authenticate Connection time: %d, search time: %d, authenticate time: %d",res.connectionTime,res.searchTime, res.authenticateTime)); 	
            System.exit(0);
        }else {
            System.exit(0);
        }
    }
}
