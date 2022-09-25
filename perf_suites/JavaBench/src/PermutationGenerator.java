

public class PermutationGenerator
{     

	// Random Permutation Generator
	long rpg_total_values = 0;
	long rpg_generator = 0;
	long rpg_prime = 0;
	long rpg_current_value = 0;

	boolean option_debug = false;

	/******************************* Constructors **************************************/

	/**
	 * Create a permutation generator for the specified number of items.
	 * @param _items The number of items in the distribution.
	 */
	public PermutationGenerator(int _items) throws Exception 
	{
		this((long)_items);
	}

	public PermutationGenerator(long items) throws Exception 
	{

		if (option_debug) { System.out.println("> init_random_permutation(" + items + " )"); }

		rpg_total_values = items;

		if ( items >= 2147483647 ) {
			throw new Exception("ERROR : init_random_permutation support only upto 1073741824 or 2^30 combinations.");
		}
		
		// Choose a prime number that is appropriate for the size of the set
		//  Values as recommended from "Quickly Generating Billion-Record Synthetic Databases" by Jim Gray, eta
		if (items <= 10) {			 /* 1 thousand */
			rpg_generator = 2;
			rpg_prime = 11;
		} else if (items <= 100) {			 /* 1 thousand */
			rpg_generator = 7;
			rpg_prime = 101;
		} else if (items <= 1000) {			 /* 1 thousand */
			rpg_generator = 26;
			rpg_prime = 1009;
		} else if (items <= 10000) {	 /* 10 thousand */
			rpg_generator = 59;
			rpg_prime = 10007;
		} else if (items <= 100000) {	/* 100 thousand */
			rpg_generator = 242;
			rpg_prime = 100003;
		} else if (items <= 1000000) {   /* 1 million */
			rpg_generator = 568;
			rpg_prime = 1000003;
		} else if (items <= 10000000) {  /* 10 million */
			rpg_generator = 1792;
			rpg_prime = 10000019;
		} else if (items <= 100000000){  /* 100 million */
			rpg_generator = 5649;
			rpg_prime = 100000007;
		} else { /* 1 billion + */
			rpg_generator = 16807;
			rpg_prime = 2147483647;
		}

		rpg_current_value = rpg_generator;
		
		if (option_debug) { System.out.println("< init_random_permutation()"); }
	}

 
	//----------------------------------------------------------------------------------------------------------
	//  Return the "next_random" element in the permutation
	public int nextInt() {
		return((int)nextLong());
	}

	public long nextLong() {
		if (option_debug) { System.out.println("> next_random_permutation ()"); }
		/****************************** random_permutation ***********************************
		* generates a unique random permutation of numbers in [1...limit] by generating
		* all elements in the Galois field Z(prime) under multiplication.
		* **********************************************************************/
		long current_value;
		do {
			current_value = (rpg_generator * rpg_current_value) % rpg_prime ;
		} while ( current_value > rpg_total_values );
		rpg_current_value = current_value;
		if (option_debug) { System.out.println("< next_random_permutation(" + current_value + ")"); }
		return(current_value);
	}

	//----------------------------------------------------------------------------------------------------------
	//  Position "next_random" to the nth element in the permutation. (element n+1 will the next element)
	public void positionInt( int n ) {
		positionLong((int)n);
	}

	public void positionLong( long n ) {
		if (option_debug) { System.out.println("> position_random_permutation(" + n + ")"); }

		long current_value = rpg_generator;		
		//  We now need to loop until we're at the proper position.
		for ( int idx = 1; idx < n; idx++ ) {
			current_value = nextLong();
		}
		rpg_current_value = current_value;
	}

	public void set_debug( Boolean debug_flag ) {
		
		option_debug = debug_flag;
		
	}

}