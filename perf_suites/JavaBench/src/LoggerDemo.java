import org.apache.log4j.Logger;



public class LoggerDemo {
	
	final static Logger logger = Logger.getLogger("JavaBench");
	
	public static void main(String[] args) {
		
		if(logger.isDebugEnabled()){
			if (logger.isDebugEnabled()) logger.debug("This is debug.");
		}
 
		if(logger.isInfoEnabled()){
			logger.info("This is info.");
		}
 
		logger.warn("This is warn.");
		logger.error("This is error.");
		logger.fatal("This is fatal.");
 

	}
}
