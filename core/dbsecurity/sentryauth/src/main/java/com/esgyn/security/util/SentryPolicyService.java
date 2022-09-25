// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016-2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.security.util;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.SentryAccessDeniedException;
import org.apache.sentry.provider.db.SentryAlreadyExistsException;
import org.apache.sentry.provider.db.SentryNoSuchObjectException;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.provider.db.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.service.thrift.TSentryRole;
import org.apache.sentry.service.thrift.SentryServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esgyn.security.analysis.PrivilegeSpec;
import com.esgyn.security.authorization.SentryConfig;
import com.esgyn.security.authorization.User;
import com.esgyn.security.authorization.EsgynDBPrivilege;
import com.esgyn.security.authorization.EsgynDBPrivilege.Privilege;
import com.esgyn.security.authorization.EsgynDBPrivilege.PrivilegeScope;
import com.esgyn.security.catalog.AuthorizationException;
import com.esgyn.security.common.InternalException;
import com.esgyn.security.thrift.TPrivilege;
import com.esgyn.security.thrift.TPrivilegeLevel;
import com.esgyn.security.thrift.TPrivilegeScope;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Wrapper around the SentryService APIs that are used by EsgynDB
 */
public class SentryPolicyService {
	private final static Logger LOG = LoggerFactory.getLogger(SentryPolicyService.class);
	private final String ACCESS_DENIED_ERROR_MSG = "User '%s' does not have privileges to execute: %s";
	private final SentryConfig config_;
	private static SentryPolicyService _instance;

	/**
	 * Wrapper around a SentryPolicyServiceClient. TODO: When SENTRY-296 is
	 * resolved we can more easily cache connections instead of opening a new
	 * connection for each request.
	 */
	class SentryServiceClient {
		private final SentryPolicyServiceClient client_;

		/**
		 * Creates and opens a new Sentry Service thrift client.
		 */
		public SentryServiceClient() throws InternalException {
			client_ = createClient();
		}

		/**
		 * Get the underlying SentryPolicyServiceClient.
		 */
		public SentryPolicyServiceClient get() {
			return client_;
		}

		/**
		 * Returns this client back to the connection pool. Can be called
		 * multiple times.
		 */
		public void close() {
			client_.close();
		}

		/**
		 * Creates a new client to the SentryService.
		 */
		private SentryPolicyServiceClient createClient() throws InternalException {
			SentryPolicyServiceClient client;
			try {
				client = SentryServiceClientFactory.create(config_.getConfig());
			} catch (Exception e) {
				throw new InternalException("Error creating Sentry Service client: ", e);
			}
			return client;
		}
	}

	public static SentryPolicyService getInstance() throws InternalException {
		return getInstance("");
	}

	public static SentryPolicyService getInstance(String overideConfigPath) throws InternalException {
		if (_instance == null) {
			try {
				String path = getSentryConfigPath();
				if (overideConfigPath != null && overideConfigPath.length() > 0) {
					path = overideConfigPath;
				}
				SentryConfig sentryConfig = new SentryConfig(path);
				sentryConfig.loadConfig();
				_instance = new SentryPolicyService(sentryConfig);
			} catch (Exception ex) {
				LOG.error("Failed to load sentry configuration file. " + ex.getMessage());
				throw new InternalException("Error creating Sentry Service client: ", ex);
			}
		}
		return _instance;
	}

	private SentryPolicyService(SentryConfig config) {
		config_ = config;
	}

	/**
	 * Drops a role.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role to drop.
	 * @param ifExists
	 *            - If true, no error is thrown if the role does not exist.
	 * @throws ImpalaException
	 *             - On any error dropping the role.
	 */
	public void dropRole(User requestingUser, String roleName, boolean ifExists) throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Dropping role: %s on behalf of: %s", roleName, requestingUser.getName()));
		}
		SentryServiceClient client = new SentryServiceClient();
		try {
			if (ifExists) {
				client.get().dropRoleIfExists(requestingUser.getShortName(), roleName);
			} else {
				client.get().dropRole(requestingUser.getShortName(), roleName);
			}
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "DROP_ROLE"));
		} catch (SentryUserException e) {
			throw new InternalException("Error dropping role: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Creates a new role.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role to create.
	 * @param ifNotExists
	 *            - If true, no error is thrown if the role already exists.
	 * @throws ImpalaException
	 *             - On any error creating the role.
	 */
	public void createRole(User requestingUser, String roleName, boolean ifNotExists) throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Creating role: %s on behalf of: %s", roleName, requestingUser.getName()));
		}
		SentryServiceClient client = new SentryServiceClient();
		try {
			client.get().createRole(requestingUser.getShortName(), roleName);
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "CREATE_ROLE"));
		} catch (SentryAlreadyExistsException e) {
			if (ifNotExists)
				return;
			throw new InternalException("Error creating role: ", e);
		} catch (SentryUserException e) {
			throw new InternalException("Error creating role: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Grants a role to a group.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role to grant to a group. Role must already exist.
	 * @param groupName
	 *            - The group to grant the role to.
	 * @throws ImpalaException
	 *             - On any error.
	 */
	public void grantRoleToGroup(User requestingUser, String roleName, String groupName) throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Granting role '%s' to group '%s' on behalf of: %s", roleName, groupName,
					requestingUser.getName()));
		}
		SentryServiceClient client = new SentryServiceClient();
		try {
			client.get().grantRoleToGroup(requestingUser.getShortName(), groupName, roleName);
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "GRANT_ROLE"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'grantRoleToGroup' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Removes a role from a group.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role name to remove.
	 * @param groupName
	 *            - The group to remove the role from.
	 * @throws InternalException
	 *             - On any error.
	 */
	public void revokeRoleFromGroup(User requestingUser, String roleName, String groupName) throws Exception {
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Revoking role '%s' from group '%s' on behalf of: %s", roleName, groupName,
					requestingUser.getName()));
		}
		SentryServiceClient client = new SentryServiceClient();
		try {
			client.get().revokeRoleFromGroup(requestingUser.getShortName(), groupName, roleName);
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "REVOKE_ROLE"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'revokeRoleFromGroup' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Grants a privilege to an existing role.
	 */
	public void grantRolePrivilege(User requestingUser, String roleName, TPrivilege privilege) throws Exception {
		grantRolePrivileges(requestingUser, roleName, Lists.newArrayList(privilege));
	}

	/**
	 * Grants privileges to an existing role.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role to grant privileges to (case insensitive).
	 * @param privilege
	 *            - The privilege to grant.
	 * @throws ImpalaException
	 *             - On any error
	 */
	public void grantRolePrivileges(User requestingUser, String roleName, List<TPrivilege> privileges)
			throws Exception {
		Preconditions.checkState(!privileges.isEmpty());
		TPrivilege privilege = privileges.get(0);
		TPrivilegeScope scope = privilege.getScope();
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Granting role '%s' '%s' privilege on '%s' on behalf of: %s", roleName,
					privilege.getPrivilege_level().toString(), scope.toString(), requestingUser.getName()));
		}
		// Verify that all privileges have the same scope.
		for (int i = 1; i < privileges.size(); ++i) {
			Preconditions.checkState(privileges.get(i).getScope() == scope,
					"All the " + "privileges must have the same scope.");
		}
		Preconditions.checkState(scope == TPrivilegeScope.COLUMN || privileges.size() == 1,
				"Cannot grant multiple " + scope + " privileges with a singe RPC to the " + "Sentry Service.");
		SentryServiceClient client = new SentryServiceClient();
		try {
			switch (scope) {
			case SERVER:
				client.get().grantServerPrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getPrivilege_level().toString(), privilege.isHas_grant_opt());
				break;
			case DATABASE:
				client.get().grantDatabasePrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getDb_name(), privilege.getPrivilege_level().toString(), privilege.isHas_grant_opt());
				break;
			case TABLE:
				client.get().grantTablePrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getDb_name(), privilege.getTable_name(), privilege.getPrivilege_level().toString(),
						privilege.isHas_grant_opt());
				break;
			case COLUMN:
				client.get().grantColumnsPrivileges(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getDb_name(), privilege.getTable_name(), getColumnNames(privileges),
						privilege.getPrivilege_level().toString(), privilege.isHas_grant_opt());
				break;
			case URI:
				client.get().grantURIPrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getUri(), privilege.isHas_grant_opt());
				break;
			}
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "GRANT_PRIVILEGE"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'grantPrivilege*' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Revokes a privilege from an existing role.
	 */
	public void revokeRolePrivilege(User requestingUser, String roleName, TPrivilege privilege) throws Exception {
		revokeRolePrivileges(requestingUser, roleName, Lists.newArrayList(privilege));
	}

	/**
	 * Revokes privileges from an existing role.
	 *
	 * @param requestingUser
	 *            - The requesting user.
	 * @param roleName
	 *            - The role to revoke privileges from (case insensitive).
	 * @param privilege
	 *            - The privilege to revoke.
	 * @throws ImpalaException
	 *             - On any error
	 */
	public void revokeRolePrivileges(User requestingUser, String roleName, List<TPrivilege> privileges)
			throws Exception {
		Preconditions.checkState(!privileges.isEmpty());
		TPrivilege privilege = privileges.get(0);
		TPrivilegeScope scope = privilege.getScope();
		if (LOG.isTraceEnabled()) {
			LOG.trace(String.format("Revoking from role '%s' '%s' privilege on '%s' on " + "behalf of: %s", roleName,
					privilege.getPrivilege_level().toString(), scope.toString(), requestingUser.getName()));
		}
		// Verify that all privileges have the same scope.
		for (int i = 1; i < privileges.size(); ++i) {
			Preconditions.checkState(privileges.get(i).getScope() == scope,
					"All the " + "privileges must have the same scope.");
		}
		Preconditions.checkState(scope == TPrivilegeScope.COLUMN || privileges.size() == 1,
				"Cannot revoke multiple " + scope + " privileges with a singe RPC to the " + "Sentry Service.");
		SentryServiceClient client = new SentryServiceClient();
		try {
			switch (scope) {
			case SERVER:
				client.get().revokeServerPrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getPrivilege_level().toString());
				break;
			case DATABASE:
				client.get().revokeDatabasePrivilege(requestingUser.getShortName(), roleName,
						privilege.getServer_name(), privilege.getDb_name(), privilege.getPrivilege_level().toString(),
						null);
				break;
			case TABLE:
				client.get().revokeTablePrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getDb_name(), privilege.getTable_name(), privilege.getPrivilege_level().toString(),
						null);
				break;
			case COLUMN:
				client.get().revokeColumnsPrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getDb_name(), privilege.getTable_name(), getColumnNames(privileges),
						privilege.getPrivilege_level().toString(), null);
				break;
			case URI:
				client.get().revokeURIPrivilege(requestingUser.getShortName(), roleName, privilege.getServer_name(),
						privilege.getUri(), null);
				break;
			}
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "REVOKE_PRIVILEGE"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'revokePrivilege*' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Returns the column names referenced in a list of column-level privileges.
	 * Verifies that all column-level privileges refer to the same table.
	 */
	private List<String> getColumnNames(List<TPrivilege> privileges) {
		List<String> columnNames = Lists.newArrayList();
		String tablePath = PrivilegeSpec.getTablePath(privileges.get(0));
		columnNames.add(privileges.get(0).getColumn_name());
		// Collect all column names and verify that they belong to the same
		// table.
		for (int i = 1; i < privileges.size(); ++i) {
			TPrivilege privilege = privileges.get(i);
			Preconditions.checkState(tablePath.equals(PrivilegeSpec.getTablePath(privilege))
					&& privilege.getScope() == TPrivilegeScope.COLUMN);
			columnNames.add(privileges.get(i).getColumn_name());
		}
		return columnNames;
	}

	/**
	 * Lists all roles granted to all groups a user belongs to.
	 */
	public List<TSentryRole> listUserRoles(User requestingUser) throws Exception {
		SentryServiceClient client = new SentryServiceClient();
		try {
			return Lists.newArrayList(client.get().listUserRoles(requestingUser.getShortName()));
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "LIST_USER_ROLES"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'listUserRoles' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Lists all roles.
	 */
	public List<TSentryRole> listAllRoles(User requestingUser) throws Exception {
		SentryServiceClient client = new SentryServiceClient();
		try {
			return Lists.newArrayList(client.get().listUserRoles(requestingUser.getShortName()));
		} catch(SentryNoSuchObjectException e1){
			//If not such object return 0 roles.
			return new ArrayList<TSentryRole>();
		}catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "LIST_ROLES"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'listRoles' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Lists all roles.
	 */
	public List<TSentryRole> listAllRoles(User requestingUser, String groupName) throws Exception {
		SentryServiceClient client = new SentryServiceClient();
		try {
			return Lists.newArrayList(client.get().listRolesByGroupName(requestingUser.getShortName(), groupName));
		} catch(SentryNoSuchObjectException e1){
			//If not such object return 0 roles.
			return new ArrayList<TSentryRole>();
		}catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "LIST_ROLES"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'listRoles' RPC to Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Lists all privileges granted to a role.
	 */
	public List<TSentryPrivilege> listRolePrivileges(User requestingUser, String roleName) throws Exception {
		SentryServiceClient client = new SentryServiceClient();
		try {
			return Lists.newArrayList(client.get().listAllPrivilegesByRoleName(requestingUser.getShortName(), roleName));
		} catch (SentryAccessDeniedException e) {
			throw new AuthorizationException(
					String.format(ACCESS_DENIED_ERROR_MSG, requestingUser.getName(), "LIST_ROLE_PRIVILEGES"));
		} catch (SentryUserException e) {
			throw new InternalException("Error making 'listAllPrivilegesByRoleName' RPC to " + "Sentry Service: ", e);
		} finally {
			client.close();
		}
	}

	/**
	 * Utility function that converts a TSentryPrivilege to an TPrivilege
	 * object.
	 */
	public static TPrivilege sentryPrivilegeToTPrivilege(TSentryPrivilege sentryPriv) {
		TPrivilege privilege = new TPrivilege();
		privilege.setServer_name(sentryPriv.getServerName());
		if (sentryPriv.isSetDbName())
			privilege.setDb_name(sentryPriv.getDbName());
		if (sentryPriv.isSetTableName())
			privilege.setTable_name(sentryPriv.getTableName());
		if (sentryPriv.isSetColumnName()) {
			privilege.setColumn_name(sentryPriv.getColumnName());
		}
		if (sentryPriv.isSetURI())
			privilege.setUri(sentryPriv.getURI());
		privilege.setScope(Enum.valueOf(TPrivilegeScope.class, sentryPriv.getPrivilegeScope().toUpperCase()));
		if (sentryPriv.getAction().equals("*")) {
			privilege.setPrivilege_level(TPrivilegeLevel.ALL);
		} else {
			privilege.setPrivilege_level(Enum.valueOf(TPrivilegeLevel.class, sentryPriv.getAction().toUpperCase()));
		}
		privilege.setCreate_time_ms(sentryPriv.getCreateTime());
		if (sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE) {
			privilege.setHas_grant_opt(true);
		} else {
			privilege.setHas_grant_opt(false);
		}
		return privilege;
	}

	/**
	 * Returns the absolute path for the sentry-site.xml
	 */
	public static String getSentryConfigPath() {
		String sentryConfDir = System.getenv("SENTRY_CNF_DIR");
		return Paths.get(sentryConfDir, "sentry-site.xml").toString();
	}
	

	/**
	 * Fetch object privileges based on group name. This method is for backward compatibility.
	 * One should use the getPrivBitMapByGroups() instead
	 * @param groupNames
	 * @param objectName
	 * @return
	 * @throws Exception
	 */
	public EsgynDBPrivilege getPrivBitMap(List<String> groupNames, String objectName) throws Exception{
		return getPrivBitMapByGroups(groupNames, objectName);
	}

	
	/**
	 * Lookup privileges on object for given user name
	 * @param userName
	 * @param objectName
	 * @return EsgynDBPrivilege
	 * @throws Exception
	 */
	public EsgynDBPrivilege getPrivBitMapByUserName(String userName, String objectName) throws Exception{
		
		List<TSentryRole> userRoles = new ArrayList<TSentryRole>();
		User user = new User(userName);
		List<TSentryRole> roles = listAllRoles(user);
		for(TSentryRole role : roles){
			if(!userRoles.contains(role)){
				userRoles.add(role);
			}
		}
		return getPrivBitMapByUserRoles(user, userRoles, objectName);
	}
	

	/**
	 * Lookup privileges on object for given list of group names
	 * @param userName
	 * @param objectName
	 * @return EsgynDBPrivilege
	 * @throws Exception
	 */
	public EsgynDBPrivilege getPrivBitMapByGroups(List<String> groupNames, String objectName) throws Exception{
		
		List<TSentryRole> allRoles = new ArrayList<TSentryRole>();
		User user = new User(System.getProperty("user.name"));
		for (String groupName : groupNames) {
			List<TSentryRole> roles = listAllRoles(user, groupName);
			for(TSentryRole role : roles){
				if(!allRoles.contains(role)){
					allRoles.add(role);
				}
			}
		}
		return getPrivBitMapByUserRoles(user, allRoles, objectName);
	}

	/**
	 * Returns and EsgynDBPrivilege object that contains bitmaps for schema
	 * level, object level and column level privileges
	 */
	public EsgynDBPrivilege getPrivBitMapByUserRoles(User user, List<TSentryRole> roles, String objectName) throws Exception {

		int schemaPrivBitmap = 0;
		int schemaPrivWithGrantBitmap = 0;
		
		int objectPrivBitmap = 0;
		int objectPrivWithGrantBitmap = 0;
		
		HashMap<String, Integer> colPrivBitmaps = new HashMap<String, Integer>();
		HashMap<String, Integer> colPrivWithGrantBitmaps = new HashMap<String, Integer>();

		List<String> schemaPrivileges = new ArrayList<String>();
		List<String> schemaPrivilegesWithGrant = new ArrayList<String>();
		
		List<String> objectPrivileges = new ArrayList<String>();
		List<String> objectPrivilegesWithGrant = new ArrayList<String>();
		
		HashMap<String, List<String>> columnPrivileges = new HashMap<String, List<String>>();
		HashMap<String, List<String>> columnPrivilegesWithGrant = new HashMap<String, List<String>>();
		
		String[] objectNameParts = crackSQLAnsiName(objectName);
		String inputSchema = "";
		String inputObject = "";
		if (objectNameParts.length > 0) {
			inputObject = objectNameParts[objectNameParts.length - 1];
			if (objectNameParts.length > 1) {
				inputSchema = objectNameParts[objectNameParts.length - 2];
				if (inputSchema.length() > 0 && inputSchema.equalsIgnoreCase("hive")) {
					inputSchema = "default";
				}
			}
		}
		if (inputObject.length() < 1) {
			throw new AuthorizationException("Input object name is empty or invalid");
		}

		for (TSentryRole role : roles) {
			String roleName = role.getRoleName();
			List<TSentryPrivilege> privileges = listRolePrivileges(user, roleName);

			for (TSentryPrivilege sentryPriv : privileges) {

				String schemaName = "";
				String tableName = "";
				String columnName = "";
				String privName = "";

				if (sentryPriv.isSetDbName()) {
					schemaName = sentryPriv.getDbName();
				}
				if (sentryPriv.isSetTableName()) {
					tableName = sentryPriv.getTableName();
				}
				if (sentryPriv.isSetColumnName()) {
					columnName = sentryPriv.getColumnName();
				}
				switch (PrivilegeScope.valueOf(sentryPriv.getPrivilegeScope())) {
				case SERVER:
					privName = parseSentryPrivAction(sentryPriv);
					if (!schemaPrivileges.contains(privName)) {
						schemaPrivileges.add(privName);
						if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
							schemaPrivilegesWithGrant.add(privName);
						}
					}else{
						if(!schemaPrivilegesWithGrant.contains(privName)){
							if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
								schemaPrivilegesWithGrant.add(privName);
							}
						}
					}
					break;
				case DATABASE:
					if (inputSchema.length() > 0 && inputSchema.equalsIgnoreCase(schemaName)) {
						privName = parseSentryPrivAction(sentryPriv);
						if (!schemaPrivileges.contains(privName)) {
							schemaPrivileges.add(privName);
							if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
								schemaPrivilegesWithGrant.add(privName);
							}
						}else{
							if(!schemaPrivilegesWithGrant.contains(privName)){
								if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
									schemaPrivilegesWithGrant.add(privName);
								}
							}
						}
					}
					break;
				case TABLE:
					if (inputSchema.length() > 0 && inputSchema.equalsIgnoreCase(schemaName)
							&& inputObject.equalsIgnoreCase(tableName)) {
						privName = parseSentryPrivAction(sentryPriv);
						if (!objectPrivileges.contains(privName)) {
							objectPrivileges.add(privName);
							if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
								objectPrivilegesWithGrant.add(privName);
							}
						}
					}
					break;
				case COLUMN:
					if (inputSchema.length() > 0 && inputSchema.equalsIgnoreCase(schemaName)
							&& inputObject.equalsIgnoreCase(tableName)) {
						privName = parseSentryPrivAction(sentryPriv);
						List<String> colPrivs = null;
						List<String> colPrivsWithGrant = null;
						
						if (columnPrivileges.containsKey(columnName)) {
							colPrivs = columnPrivileges.get(columnName);
							colPrivsWithGrant = columnPrivilegesWithGrant.get(columnName);
						} else {
							colPrivs = new ArrayList<String>();
							columnPrivileges.put(columnName, colPrivs);
							
							colPrivsWithGrant = new ArrayList<String>();
							columnPrivilegesWithGrant.put(columnName, colPrivsWithGrant);
						}
						if (!colPrivs.contains(privName)) {
							colPrivs.add(privName);
							if(sentryPriv.isSetGrantOption() && sentryPriv.getGrantOption() == TSentryGrantOption.TRUE){
								colPrivsWithGrant.add(privName);
							}
						}
					}
					break;
				default:
					LOG.warn("Unknown PrivilegeScope: " + PrivilegeScope.valueOf(sentryPriv.getPrivilegeScope()));
					break;
				}
			}
		}
		
		EsgynDBPrivilege esgynPrivilege = new EsgynDBPrivilege();

		for (String privName : schemaPrivileges) {
			if (privName.equals("ALL")) {
				schemaPrivBitmap = Privilege.getPrivilege(privName);
				break;
			} else {
				schemaPrivBitmap += Privilege.getPrivilege(privName);
			}
		}
		LOG.debug("schema priv bitmap : " + schemaPrivBitmap);
		esgynPrivilege.setSchemaPrivBitmap(schemaPrivBitmap);

		for (String privName : schemaPrivilegesWithGrant) {
			if (privName.equals("ALL")) {
				schemaPrivWithGrantBitmap = Privilege.getPrivilege(privName);
				break;
			} else {
				schemaPrivWithGrantBitmap += Privilege.getPrivilege(privName);
			}
		}
		LOG.debug("schema priv with grant bitmap : " + schemaPrivWithGrantBitmap);
		esgynPrivilege.setSchemaPrivWithGrantBitmap(schemaPrivWithGrantBitmap);

		for (String privName : objectPrivileges) {
			if (privName.equals("ALL")) {
				objectPrivBitmap = Privilege.getPrivilege(privName);
				break;
			} else {
				objectPrivBitmap += Privilege.getPrivilege(privName);
			}
		}
		LOG.debug("object priv bitmap : " + objectPrivBitmap);
		esgynPrivilege.setObjectPrivBitmap(objectPrivBitmap);

		for (String privName : objectPrivilegesWithGrant) {
			if (privName.equals("ALL")) {
				objectPrivWithGrantBitmap = Privilege.getPrivilege(privName);
				break;
			} else {
				objectPrivWithGrantBitmap += Privilege.getPrivilege(privName);
			}
		}
		LOG.debug("object priv with grant bitmap : " + objectPrivWithGrantBitmap);
		esgynPrivilege.setObjectPrivWithGrantBitmap(objectPrivWithGrantBitmap);

		for (Entry<String, List<String>> entry : columnPrivileges.entrySet()) {
			String colName = entry.getKey();
			List<String> privNames = entry.getValue();
			int colBitmap = 0;
			for (String privName : privNames) {
				if (privName.equals("ALL")) {
					colBitmap = Privilege.getPrivilege(privName);
					break;
				} else {
					colBitmap += Privilege.getPrivilege(privName);
				}
			}
			colPrivBitmaps.put(colName, colBitmap);
			LOG.debug("Column privilege : " +colName + " : " + colBitmap);
		}
		esgynPrivilege.setColPrivBitmaps(colPrivBitmaps);
		
		for (Entry<String, List<String>> entry : columnPrivilegesWithGrant.entrySet()) {
			String colName = entry.getKey();
			List<String> privNames = entry.getValue();
			int colWithGrantBitmap = 0;
			for (String privName : privNames) {
				if (privName.equals("ALL")) {
					colWithGrantBitmap = Privilege.getPrivilege(privName);
					break;
				} else {
					colWithGrantBitmap += Privilege.getPrivilege(privName);
				}
			}
			colPrivWithGrantBitmaps.put(colName, colWithGrantBitmap);
			LOG.debug("Column privilege with Grant : " + colName + " : " + colWithGrantBitmap);
		}
		esgynPrivilege.setColPrivWithGrantBitmaps(colPrivWithGrantBitmaps);
		return esgynPrivilege;
	}

	/**
	 * Parse the sentry privilege
	 */
	private String parseSentryPrivAction(TSentryPrivilege sentryPriv) {
		String privName = "";
		if (sentryPriv.getAction().equals("*")) {
			privName = "ALL";
		} else {
			privName = sentryPriv.getAction().toUpperCase();
		}
		return privName;
	}

	/**
	 * Break an ansi SQL name to name parts
	 * 
	 * @param sentryPriv
	 * @return
	 */
	private String[] crackSQLAnsiName(String ansiName) {
		boolean inQuotes = false;
		int beginOffset = 0;
		List<String> parts = new ArrayList<String>();

		for (int currentOffset = 0; currentOffset < ansiName.length(); currentOffset++) {
			char aCharacter = ansiName.charAt(currentOffset);

			switch (aCharacter) {
			case '"': {
				inQuotes = !inQuotes;
				break;
			}
			case '.': {
				if (!inQuotes) {
					parts.add(ansiName.substring(beginOffset, currentOffset));
					beginOffset = currentOffset + 1;
				}
				break;
			}
			default: {
				break;
			}
			}
		}

		parts.add(ansiName.substring(beginOffset));
		String[] result = new String[parts.size()];
		return parts.toArray(result);
	}

}
