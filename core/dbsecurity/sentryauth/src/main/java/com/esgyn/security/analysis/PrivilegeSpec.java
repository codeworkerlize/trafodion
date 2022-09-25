package com.esgyn.security.analysis;


import com.esgyn.security.thrift.TPrivilege;
import com.esgyn.security.thrift.TPrivilegeScope;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class PrivilegeSpec {

	 public static String getTablePath(TPrivilege privilege) {
		    Preconditions.checkState(privilege.getScope() == TPrivilegeScope.COLUMN);
		    Joiner joiner = Joiner.on(".");
		    return joiner.join(privilege.getServer_name(), privilege.getDb_name(),
		        privilege.getTable_name());
		  }
}
