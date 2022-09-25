// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

public class NamedTenantNodes {

        public NamedTenantNodes(String name, TenantNodes nodes) {
            this(name, nodes, "", "");
        }
        public NamedTenantNodes(String name,
                                TenantNodes nodes,
                                String defaultSchema,
                                String sessionLimit) {
                this.name = name;
                tenantNodes = nodes;
                this.defaultSchema = defaultSchema;
                this.sessionLimit = sessionLimit;
        }
	public String getName() {
		return name;
	}
        public TenantNodes getTenantNodes() {
                return tenantNodes;
        }
        public String getDefaultSchema() {
                return defaultSchema;
        }
        public String getSessionLimit() {
                return sessionLimit;
        }
	public void setName(String name) {
		this.name = name;
	}
	public void setTenantNodes(TenantNodes nodes) {
		tenantNodes = nodes;
	}
	public void setSessionLimit(String limit) {
	        sessionLimit = limit;
	}
        @Override
        public boolean equals(Object anObject) {
            if (anObject instanceof NamedTenantNodes) {
                NamedTenantNodes other = (NamedTenantNodes) anObject;

                return
                    name.equals(other.getName()) &&
                    tenantNodes.equals(other.getTenantNodes()) &&
                    defaultSchema.equals(other.getDefaultSchema()) &&
                    sessionLimit.equals(other.getSessionLimit());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode() ^ tenantNodes.hashCode();
        }

	private String name;
        private TenantNodes tenantNodes;
        private String defaultSchema;
        private String sessionLimit;
}
