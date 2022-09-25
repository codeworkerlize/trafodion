
public class GetYcsbSplitTableCommand {

	public static void main(String args[]) {

		String table = null;
		int splitpoint = 0;
		String splitkey = null;
		int partitions = 64;
		int sleepTime = 1;

		for ( int indx = 0; indx < args.length; indx++ ) {
			switch ( args[indx] ) {
				case "table" : table =  args[++indx]; break;
				case "splitpoint" : splitpoint = Integer.parseInt(args[++indx]); break;
				case "partitions" : partitions = Integer.parseInt(args[++indx]); break;
				case "sleep" : sleepTime = Integer.parseInt(args[++indx]); break;
				default: {
					System.err.println("ERROR : Invalid option specified ( option = " + args[indx] + " )");
				}
			}
		}

		if ( splitpoint == 0 ) {
			int keysperpartition = ( 999999 - 100000 ) / partitions;

			for ( int idx = 1; idx < partitions; idx++ ) {
				splitkey = "user" + String.format("%6d", 100000 + idx * keysperpartition);

			    System.out.println("split '" + table + "'.to_java_bytes, '" + splitkey + "'.to_java_bytes");
			    System.out.println("sleep " + sleepTime);
			}
		} else {
			splitkey = "user" + String.format("%4d", splitpoint);
		    System.out.println("split '" + table + "'.to_java_bytes, '" + splitkey + "'.to_java_bytes");
		}

		System.out.println("sleep " + sleepTime * 2);
	}
}

