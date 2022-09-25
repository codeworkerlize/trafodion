// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016-2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.security.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import com.esgyn.security.authorization.EsgynDBPrivilege;
import com.esgyn.security.authorization.User;
import com.esgyn.security.thrift.TPrivilegeLevel;
import com.esgyn.security.thrift.TPrivilegeScope;


/**
 * 
 * Client program to test interface to Sentry
 *
 */
public class AuthTest {
	public static SentryPolicyService sentryService = null;

	public static void main(String[] args) {
		if(args.length < 2){
			System.out.println("You need to specify the group name and table names as input");
			System.out.println("");
			System.out.println("Usage : AuthTest <group name> <tablename> [<sentry site path>");
			return;
		}
		try {
			String groupName = args[0];
			String tableName = args[1];
			String sentrySiteConfPath = "";
			if(args.length < 3){
				sentrySiteConfPath = SentryPolicyService.getSentryConfigPath();
				System.out.println(sentrySiteConfPath);
			}else{
				sentrySiteConfPath = args[2];
			}
			testSentry(groupName, sentrySiteConfPath);
			parseSentryPrivileges(groupName, tableName, sentrySiteConfPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void parseSentryPrivileges(String groupName, String tableName, String sentrySiteConfPath)
			throws Exception {
		//AuthorizationConfig authzConfig = AuthorizationConfig.createHadoopGroupAuthConfig("server1", null, sentrySiteConfPath);
		//authzConfig.validateConfig();
		List<String> groupNames = new ArrayList<String>();
		groupNames.add(groupName);
		
		sentryService = SentryPolicyService.getInstance(sentrySiteConfPath);
		EsgynDBPrivilege privilege = sentryService.getPrivBitMapByGroups(groupNames, tableName);
		
		System.out.println("Schema Privilege : " + privilege.getSchemaPrivBitmap());
		System.out.println("Schema Privilege With Grant : " + privilege.getSchemaPrivWithGrantBitmap());
		
		System.out.println("Object Privilege : " + privilege.getObjectPrivBitmap());
		System.out.println("Object Privilege With Grant : " + privilege.getObjectPrivWithGrantBitmap());
		
		HashMap<String, Integer> colBitmaps = privilege.getColPrivBitmaps();
		HashMap<String, Integer> colWithGrantBitmaps = privilege.getColPrivWithGrantBitmaps();
		for (Entry<String, Integer> entry : colBitmaps.entrySet()){
			System.out.println("Column Name : " + entry.getKey() + " : " + entry.getValue());
		}
		for (Entry<String, Integer> entry : colWithGrantBitmaps.entrySet()){
			System.out.println("Column Name With Grant : " + entry.getKey() + " : " + entry.getValue());
		}

	}
	
	public static void testSentry(String groupName, String sentrySiteConfPath)
			throws Exception {
		sentryService = SentryPolicyService.getInstance(sentrySiteConfPath);

		User user = new User(System.getProperty("user.name"));
		List<TSentryRole> roles = sentryService.listAllRoles(user, groupName);
		for(TSentryRole role : roles){
			String roleName = role.getRoleName();
			System.out.println("Role Name : " + roleName);
			List<TSentryPrivilege> privileges = sentryService.listRolePrivileges(user, roleName);
			for(TSentryPrivilege sentryPriv : privileges){
				//System.out.println("Privileges : " + sentryPriv);
				System.out.println(Enum.valueOf(TPrivilegeScope.class, sentryPriv.getPrivilegeScope().toUpperCase()));
				
			    if (sentryPriv.isSetDbName()){
			    	System.out.println("Schema Name " + sentryPriv.getDbName());
			    }
			    if (sentryPriv.isSetTableName()){
			    	System.out.println("Table Name " + sentryPriv.getTableName());
			    }
			    if (sentryPriv.isSetColumnName()) {
			    	System.out.println("Column Name " + sentryPriv.getColumnName());
			    }
			    if (sentryPriv.getAction().equals("*")) {
			    	System.out.println("Privilege : " + TPrivilegeLevel.ALL);
			    } else {
			    	System.out.println("Privilege : " + Enum.valueOf(TPrivilegeLevel.class, sentryPriv.getAction().toUpperCase()));
			    }
			    System.out.println("Create Time : " + sentryPriv.getCreateTime());
			    if (sentryPriv.isSetGrantOption() &&
			        sentryPriv.getGrantOption() == TSentryGrantOption.TRUE) {
			    	System.out.println("Grant Option : " + true);
			    } else {
			    	System.out.println("Grant Option : " + false);
			    }
				System.out.println("");
			}
			System.out.println("");
		}
	}

}
