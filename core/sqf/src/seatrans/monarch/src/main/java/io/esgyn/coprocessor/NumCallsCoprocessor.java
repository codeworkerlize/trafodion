package io.esgyn.coprocessor;

import io.ampool.monarch.table.internal.MRowScanner;
import io.ampool.monarch.table.Scan;
import io.ampool.monarch.table.MTableRegion;
import io.ampool.monarch.table.coprocessor.MCoprocessor;
import io.ampool.monarch.table.coprocessor.MCoprocessorContext;
import io.ampool.monarch.table.coprocessor.MExecutionRequest;
import io.ampool.monarch.table.exceptions.MCoprocessorException;
import io.ampool.monarch.table.filter.KeyOnlyFilter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NumCallsCoprocessor extends MCoprocessor {

    static final Logger logger = LogManager.getLogger(NumCallsCoprocessor.class.getName());

    long m_calls = 0;

    public  NumCallsCoprocessor() {
    }

    public long numCalls(MCoprocessorContext context) 
    {
	logger.info("NumCallsCoprocessor (method: numCalls) Entry"
		    + ", m_calls:" + m_calls);

        MExecutionRequest request = context.getRequest();

	logger.info("NumCallsCoprocessor (method: numCalls)"
		    + ", Table Name: " + context.getTable().getName()
		    );

	try {
	    MTableRegion lv_region = context.getMTableRegion();
	    
	    logger.info("NumCallsCoprocessor (method: numCalls)"
			+ ", Region: " + lv_region
			);
	}
	catch (Throwable et) {
	    logger.error("NumCallsCoprocessor (method: numCalls) exception"
			 , et
			 );
	}

        return ++m_calls;
    }

    @Override
	public String getId() {
	return this.getClass().getName();
    }

}
