package org.trafodion.sql.parquet;

import org.apache.log4j.Logger;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;

import static org.apache.parquet.Preconditions.checkArgument;
import static org.apache.parquet.Preconditions.checkNotNull;

public class TrafFilter extends FilterCompat.FilterPredicateCompat {

    static Logger logger = Logger.getLogger(TrafFilter.class.getName());;

    public static FilterCompat.Filter get(FilterPredicate filterPredicate) {
	checkNotNull(filterPredicate, "filterPredicate");

	if (logger.isTraceEnabled()) logger.trace("TrafFilter.get()"
						  + ", Filtering using predicate: " + filterPredicate
						  );

	// rewrite the predicate to not include the not() operator
	FilterPredicate collapsedPredicate = LogicalInverseRewriter.rewrite(filterPredicate);

	if (logger.isTraceEnabled()) {
	    if (!filterPredicate.equals(collapsedPredicate))
		logger.trace("TrafFilter.get"
			     + ", predicate has been collapsed to: " + collapsedPredicate
			     );
	}

	return new TrafFilter(collapsedPredicate);
    }

    public TrafFilter(FilterPredicate filterPredicate) {
	super(filterPredicate);
    }
    
    @Override
    public <R> R accept(FilterCompat.Visitor<R> visitor) {
	if (logger.isTraceEnabled()) logger.trace("TF.accept()" 
						  + ", visitor: " + visitor
						  );
	R lv_ret = visitor.visit(this);
	if (logger.isTraceEnabled()) 
	    logger.trace("TF.accept()"
			 + ", ret: " + lv_ret.getClass().getName()
			 );
	return lv_ret;
    }
    
}
