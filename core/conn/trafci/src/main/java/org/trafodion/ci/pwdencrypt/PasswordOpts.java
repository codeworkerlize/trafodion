/**********************************************************************
// @@@ START COPYRIGHT @@@
//
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
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
package org.trafodion.ci.pwdencrypt;
import java.util.Arrays;
import java.util.Vector;

public class PasswordOpts {

	/**
	 * Max length of encryptable text.
	 */
	private final int maxtextlen = 128;

	/**
	 * referencename.
	 */
	private String ref;
	/**
	 * text to be add/deleted.
	 */
	private String text;
	/**
	 * directory of the log file.
	 */
	private String dir;
	/**
	 * file name of the log file.
	 */
	private String file;
	/**
	 * work type to be done.
	 */
	private String wktype;
      
	static boolean doTrace = Boolean.getBoolean("trafci.enableTrace");
	
    

	public static final Vector<String> OPTIONTYPES = new Vector<String>(
			Arrays.asList("option", "username", "password", "logdir","logfile","help"));

	/**
	 * @param optsMap
	 */
	public PasswordOpts() {
		ref = null;
		text = null;
		dir = null;
		file = null;
		wktype = null;

	}

	/**
	 * Display help.
	 *
	 */
	private void displayHelp() {
		System.out.println(" \nUsage: \n" +
					"  ciencr.sh -o {add -u <username> -p <password> | del -u <username> } \n");
	}

	/**
	 * Parse Commandline Args.
	 * @param args The command to parsed
	 */
	public final void parseCommandLine(final String[] args) {

			
		if (args.length < 2 ) {			
			displayHelp();
			if(!(args.length > 0 && 
				(args[0].contains("h") || 
				args[0].equalsIgnoreCase("help")))){
				System.exit(6);
			}
			System.exit(0);
		}
		
		char flag;

		boolean hasO = false;
		for (int i = 0; i < args.length; i++) {
			flag = '?';
			try {
				flag = args[i].charAt(1)=='-'? args[i].charAt(2): args[i].charAt(1);
				if(args[i].length()> 2 && !(OPTIONTYPES.contains(args[i].substring(1).toLowerCase()))){
						if (doTrace) 
							System.out.println("Invalid option");
						displayHelp();
						System.exit(5);
				}
			} catch (Exception e) {
				StringBuffer msg = new StringBuffer("\n\tError message: ");
				msg.append(e.getMessage());
				if (doTrace) { 
					e.printStackTrace();
				}
				System.exit(5);
			}
			switch (flag) {
			case 'o':
				if (ArgMap.ARGMAP.get('o').isArgRequired()) {
					i++;
					if (WorkTypes.WORKTYPES.contains(args[i].toLowerCase())) {
						wktype = args[i];
					}else{
						if (doTrace)
							System.out.println("Option '"+args[i]+"' is not valid.");
						displayHelp();
						System.exit(5);
					}
				}
				if(wktype == null){
					if (doTrace)
						System.out.println("Option is required.");
					displayHelp();
					System.exit(5);
				}
				hasO = true;
				break;

			case 'u':
				if (ArgMap.ARGMAP.get('u').isArgRequired()) {
					i++;
					ref = args[i];
					if(ref.charAt(0)=='-' || ref == null){	 if (doTrace)
						System.out.println("Arg is u");
						displayHelp();
						System.exit(5);
					}
				}
				break;

			case 'p':
				if (ArgMap.ARGMAP.get('p').isArgRequired()) {
					i++;
					if (i>=args.length){
						displayHelp();
						System.exit(5);
					}						
					text = args[i];
					if (text == null || text.length() > maxtextlen) {
						if (doTrace)
							System.out.println("Arg is p");
						displayHelp();
						System.exit(5);
					}
				}
				break;

			case 'l':
				if (ArgMap.ARGMAP.get('l').isArgRequired()) {
					i++;
					dir = args[i];
				}
				break;
			case 'f':
				if (ArgMap.ARGMAP.get('f').isArgRequired()) {
					i++;
					file = args[i];
				}
				break;
			case 'h':
				displayHelp();
				System.exit(0);
				break;
			default:
				if (doTrace) {
					System.out.println("Default arg");
					displayHelp();
				}

			break;
			}

		}
		if (!hasO) {
			if (doTrace)
				System.out.println("Error: '-o' is required");
			displayHelp();
			System.exit(0);
		}
		checkArgs();
	}

	/**
	 * Checks the required args for add and del worktypes.
	 *
	 */
	private void checkArgs() {

			if (wktype.equalsIgnoreCase(WorkTypes.ADD)) {
				if (ref == null || text == null) {
					if (doTrace)
					  System.out.println("Errors in ADD WrkType ");
				   displayHelp();
				   System.exit(5);
				}
			}
			else if (wktype.equalsIgnoreCase(WorkTypes.DEL)) {
				if (ref == null) {
					if (doTrace)
					  System.out.println("Errors in del WrkType ");
				   displayHelp();
				   System.exit(5);
				}
			}
	}

	/**
	 * @return the dir
	 */
	public final String getDir() {
		return dir;
	}

	/**
	 * @return the file
	 */
	public final String getFile() {
		return file;
	}

	/**
	 * @return the ref
	 */
	public final String getRef() {
		return ref;
	}

	/**
	 * @return the text
	 */
	public final String getText() {
		return text;
	}

	/**
	 * @return the wktype
	 */
	public final String getWktype() {
		return wktype;
	}
}
