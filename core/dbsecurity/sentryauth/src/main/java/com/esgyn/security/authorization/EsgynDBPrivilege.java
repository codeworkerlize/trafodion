// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.security.authorization;

import java.util.HashMap;

import com.esgyn.security.util.SentryPolicyService;

public class EsgynDBPrivilege {
	public static SentryPolicyService sentryService = null;
	
	private int schemaPrivBitmap = 0;
	private int schemaPrivWithGrantBitmap = 0;
	
	private int objectPrivBitmap = 0;
	private int objectPrivWithGrantBitmap = 0;
	
	HashMap<String, Integer> colPrivBitmaps = new HashMap<String, Integer>();
	HashMap<String, Integer> colPrivWithGrantBitmaps = new HashMap<String, Integer>();

	public int getSchemaPrivWithGrantBitmap() {
		return schemaPrivWithGrantBitmap;
	}

	public void setSchemaPrivWithGrantBitmap(int schemaPrivWithGrantBitmap) {
		this.schemaPrivWithGrantBitmap = schemaPrivWithGrantBitmap;
	}

	public int getObjectPrivWithGrantBitmap() {
		return objectPrivWithGrantBitmap;
	}

	public void setObjectPrivWithGrantBitmap(int objectPrivWithGrantBitmap) {
		this.objectPrivWithGrantBitmap = objectPrivWithGrantBitmap;
	}

	public HashMap<String, Integer> getColPrivWithGrantBitmaps() {
		return colPrivWithGrantBitmaps;
	}

	public void setColPrivWithGrantBitmaps(HashMap<String, Integer> colPrivWithGrantBitmaps) {
		this.colPrivWithGrantBitmaps = colPrivWithGrantBitmaps;
	}

	public int getSchemaPrivBitmap() {
		return schemaPrivBitmap;
	}

	public void setSchemaPrivBitmap(int schemaPrivBitmap) {
		this.schemaPrivBitmap = schemaPrivBitmap;
	}

	public int getObjectPrivBitmap() {
		return objectPrivBitmap;
	}

	public void setObjectPrivBitmap(int objectPrivBitmap) {
		this.objectPrivBitmap = objectPrivBitmap;
	}

	public HashMap<String, Integer> getColPrivBitmaps() {
		return colPrivBitmaps;
	}

	public void setColPrivBitmaps(HashMap<String, Integer> colPrivBitmaps) {
		this.colPrivBitmaps = colPrivBitmaps;
	}

	public static enum PrivilegeScope {
		SERVER, URI, DATABASE, TABLE, COLUMN
	}

	public enum Privilege {
		NONE(0), SELECT(1), INSERT(2), DELETE(4), UPDATE(8), USAGE(16), REFERENCES(32), EXECUTE(64), ALL(127);

		private int priv = 0;

		Privilege(int val) {
			this.priv = val;
		}

		public int getPrivilege() {
			return priv;
		}

		public static int getPrivilege(String name) {
			for (Privilege p : Privilege.values()) {
				if (p.name().equals(name)) {
					return p.getPrivilege();
				}
			}
			return 0;
		}
	}
}
