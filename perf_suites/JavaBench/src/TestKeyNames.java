

import com.yahoo.ycsb.workloads.CoreWorkload;

public class TestKeyNames {

	public static void main(String args[]) {

		String command_line = "";
		for (int idx = 0; idx < args.length; idx++) {
			command_line = command_line + args[idx] + " ";
		}
		System.out.println("testprogram " + command_line);

		int count = 1;
		boolean debug = false;
		
		for ( int indx = 0; indx < args.length; indx++ ) {
			switch ( args[indx] ) {
				case "count" : count =  Integer.parseInt(args[++indx]); break;
				case "debug" : debug = true; break;
				default: {
					System.out.println("ERROR : Invalid option specified ( option = " + args[indx] + " )");
				}
			}
		}
		
		System.out.println(" debug = " + debug); 

		String key_value;

		CoreWorkload workload = new CoreWorkload();
		
		for (long i = 0; i < count; i++) {
			key_value = workload.buildKeyName(i);
			System.out.println(" i = " + i + " , key = " + key_value); 
		}
	}
}

