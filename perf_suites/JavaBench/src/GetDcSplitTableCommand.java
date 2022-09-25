import java.nio.ByteBuffer;

public class GetDcSplitTableCommand {

	public static void main(String args[]) {

		String table = null;
		int splitpoint = 0;
		int scalefactor = 1024;
		int partitions = 64;
		int sleepTime = 1;
		boolean option_serialize = false;
		int rowsperscale = 100000;
			
		for ( int indx = 0; indx < args.length; indx++ ) {
			switch ( args[indx] ) {
				case "table" : table =  args[++indx]; break;
				case "splitpoint" : splitpoint = Integer.parseInt(args[++indx]); break;
				case "serialize" : option_serialize = true; break;
				case "scalefactor" : scalefactor = Integer.parseInt(args[++indx]); break; 
				case "rowsperscale" : rowsperscale = Integer.parseInt(args[++indx]); break; 
				case "partitions" : partitions = Integer.parseInt(args[++indx]); break; 
				case "sleep": sleepTime = Integer.parseInt(args[++indx]); break;
				default: {
					System.err.println("ERROR : Invalid option specified ( option = " + args[indx] + " )");
				}
			}
		}
		
		if ( splitpoint == 0 ) {
			int scaleremaining = scalefactor;

			for ( int idx = 1; idx < partitions; idx++ ) {
				int scaleperpartition = ( scaleremaining / ( partitions - idx + 1 ));
				splitpoint = splitpoint + ( scaleperpartition * rowsperscale );
				scaleremaining = scaleremaining - scaleperpartition;
			    byte[] keyvalue = ByteBuffer.allocate(4).putInt(splitpoint).array();
			    if ( option_serialize ) {
				    keyvalue[0] = (byte) ( keyvalue[0] ^ -0x80 );
			    }
		        String keyBytesStr = "";
			    for (int i=0; i < keyvalue.length; i++) {
			    	keyBytesStr = keyBytesStr + "\\x" + String.format("%02X", keyvalue[i]);
			    }
			    System.out.println("split '" + table + "'.to_java_bytes, \"" + keyBytesStr + "\".to_java_bytes");
			    System.out.println("sleep " + sleepTime);
			}
		} else {
		    byte[] keyvalue = ByteBuffer.allocate(4).putInt(splitpoint).array();
		    if ( option_serialize ) {
			    keyvalue[0] = (byte) ( keyvalue[0] ^ -0x80 );
		    }
	        String keyBytesStr = "";
		    for (int i=0; i < keyvalue.length; i++) {
		    	keyBytesStr = keyBytesStr + "\\x" + String.format("%02X", keyvalue[i]);
		    }
		    System.out.println("split '" + table + "'.to_java_bytes, '" + keyBytesStr + "'.to_java_bytes");
		}
		
		System.out.println("sleep " + sleepTime * 2);
	}
}
