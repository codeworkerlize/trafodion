import java.util.Random;

import org.apache.log4j.Logger;


public class RandomGenerator {

	final static int INVALID_STREAMNUMBER = -1;
	public int stream_number = INVALID_STREAMNUMBER;

	final int MAX_CONNECT_ATTEMPTS = 100;
	final int REPORT_CONNECT_ATTEMPTS = 10;

	public boolean option_controlrandom = false;

	Random generator = new Random();

	final static Logger logger = Logger.getLogger("JavaBench");

	// Random Permutation Generator
	int rpg_total_values = 0;
	int rpg_generator = 0;
	int rpg_prime = 0;
	int rpg_position = 1;
//	public static int rpg_init_value;

	//----------------------------------------------------------------------------------------------------------

	int generateRandomInteger(int lowvalue, int highvalue) {
		int randnum = generator.nextInt((highvalue - lowvalue) + 1) + lowvalue;
		return randnum;
	}

	//----------------------------------------------------------------------------------------------------------

	String generateRandomAString (int strlength) {
		final String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		final int N = alphabet.length();
		String random_string = "";
		for ( int indx = 0; indx < strlength; indx++ ) {
			random_string = random_string + alphabet.charAt(generator.nextInt(N));
		}
		return random_string;
	}

	
	//----------------------------------------------------------------------------------------------------------

		int NURand(int A, int x, int y, int C) throws Exception {
			return (((generateRandomInteger(0,A) | generateRandomInteger(x,y)) + C) % ( y - x + 1)) + x;
		}

	//----------------------------------------------------------------------------------------------------------

		String generateRandomLastName(int cust_id) throws Exception
		{
			String[] substrings = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING" };
			String last_name = substrings[(cust_id/100)%100] + substrings[(cust_id/10)%10]  + substrings[cust_id%10];
			return last_name;
		}

	//----------------------------------------------------------------------------------------------------------

		//  Random Permutation Generator
		
		int next_random_permutation() {
			int current_value = rpg_position;
			
			if (logger.isDebugEnabled()) logger.debug(stream_number + "> next_random_permutation (" + current_value + ")");
			/****************************** random_permutation ***********************************
			* generates a unique random permutation of numbers in [1...limit] by generating
			* all elements in the Galois field Z(prime) under multiplication.
			* **********************************************************************/
			do {
				long long_seed = (rpg_generator * current_value) % rpg_prime ;
				current_value = (int) long_seed;
			} while ( current_value > rpg_total_values );

			rpg_position = current_value;
			if (logger.isDebugEnabled()) logger.debug(stream_number + "< next_random_permutation(" + current_value + ")");
			return (current_value);
		}

	//----------------------------------------------------------------------------------------------------------

		void position_random_permutation( int loops ) throws Exception {
			if (logger.isDebugEnabled()) logger.debug(stream_number + "> position_random_permutation (" + loops + ")");

			int position = 0;		// initialize seed
			//  We now need to loop until we're at the proper position.
			for ( int idx = 0; idx < loops; idx++ ) {
				position = next_random_permutation();
			}

			if (logger.isDebugEnabled()) logger.debug(stream_number + "< position_random_permutation(" + position + ")");

		}

	//----------------------------------------------------------------------------------------------------------

		void init_random_permutation( int totrecs ) throws Exception {
			if (logger.isDebugEnabled()) logger.debug(stream_number + "> init_random_permutation(" + totrecs + " )");

			rpg_total_values = totrecs;

			// Choose a prime number that is appropriate for the size of the set
			if (totrecs <= 1000) {			 /* 1 thousand */
				rpg_generator = 279;
				rpg_prime = 1009;
			} else if (totrecs <= 10000) {	 /* 10 thousand */
				rpg_generator = 2969;
				rpg_prime = 10007;
			} else if (totrecs <= 100000) {	/* 100 thousand */
				rpg_generator = 21395;
				rpg_prime = 100003;
			} else if (totrecs <= 1000000) {   /* 1 million */
				rpg_generator = 21395;
				rpg_prime = 1000003;
			} else if (totrecs <= 10000000) {  /* 10 million */
				rpg_generator = 213957;
				rpg_prime = 10000019;
			} else if (totrecs <= 100000000){  /* 100 million */
				rpg_generator = 2139575;
				rpg_prime = 100000007;
			} else if (totrecs <= 1000000000){ /* 1 billion */
				rpg_generator = 2139575;
				rpg_prime = 1000000007;
			} else if (totrecs <= 1073741824){ /* 2^30 */
				rpg_generator = 2139575;
				rpg_prime = 1073741827;
			} else {
				throw new Exception("ERROR : init_random_permutation support only upto 1073741824 or 2^30 combinations.");
			}

			rpg_position = rpg_generator;
			
			position_random_permutation( 1 );
			
			if (logger.isDebugEnabled()) logger.debug(stream_number + "< init_random_permutation()");
		}

	//----------------------------------------------------------------------------------------------------------

	//  TPC-H generators
		
		String generateRegionName() throws Exception
		{
			String[] regions = { "AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST" };
			String r_name = regions[generateRandomInteger( 0, 4)];
			return r_name;
		}
		
		String generateNationName() throws Exception
		{
			String[] nations = { "ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT",
					"ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA",
					"IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
					"MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
					"SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES" };
			String n_name = nations[generateRandomInteger( 0, 24)];
			return n_name;
		}
		
		String generateMktSegment() throws Exception
		{
			String[] segments = { "AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD" };
			String segment = segments[generateRandomInteger( 0, 4)];
			return segment;
			
		}

		String generateColor() throws Exception
		{
			String[] p_name_words = {
					"almond","antique","aquamarine","azure","beige",
					"bisque","black","blanched","blue","blush",
					"brown","burlywood","burnished","chartreuse","chiffon",
					"chocolate","coral","cornflower","cornsilk","cream",
					"cyan","dark","deep","dim","dodger",
					"drab","firebrick","floral","forest","frosted",
					"gainsboro","ghost","goldenrod","green","grey",
					"honeydew","hot","indian","ivory","khaki",
					"lace","lavender","lawn","lemon","light",
					"lime","linen","magenta","maroon","medium",
					"metallic","midnight","mint","misty","moccasin",
					"navajo","navy","olive","orange","orchid",
					"pale","papaya","peach","peru","pink",
					"plum","powder","puff","purple","red",
					"rose","rosy","royal","saddle","salmon",
					"sandy","seashell","sienna","sky","slate",
					"smoke","snow","spring","steel","tan",
					"thistle","tomato","turquoise","violet","wheat",
					"white","yellow"
				};
			String color = p_name_words[generateRandomInteger( 0, 91)];
			return color;
			
		}

		String generateShipMode() throws Exception
		{
			String[] shipmodes = { "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB" };
			String ship_mode = shipmodes[generateRandomInteger( 0, 6)];
			return ship_mode;
		}

		String generateContainer() throws Exception
		{
			String[] syllable1 = { "SM", "LG", "MED", "JUMBO", "WRAP" };
			String[] syllable2 = { "CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM" };
			String container = syllable1[generateRandomInteger( 0, 4)] 
							+ " " + syllable2[generateRandomInteger( 0, 7)];
			return container;
		}

		String generateBrand() throws Exception
		{
			String brand = "Brand#" + generateRandomInteger(1,5) + generateRandomInteger(1,5);
			return brand;
		}

		String generatePartType() throws Exception
		{
			String[] syllable1 = { "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO" };
			String[] syllable2 = { "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED" };
			String[] syllable3 = { "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
			String p_type = syllable1[generateRandomInteger( 0, 5)] 
					+ " " + syllable2[generateRandomInteger( 0, 4)] 
					+ " " + syllable3[generateRandomInteger( 0, 4)];
			return p_type;
		}

		String generatePartType2() throws Exception
		{
			String[] syllable1 = { "STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO" };
			String[] syllable2 = { "ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED" };
			String p_type = syllable1[generateRandomInteger( 0, 5)] 
					+ " " + syllable2[generateRandomInteger( 0, 4)] ;
			return p_type;
		}

		String generatePartType3() throws Exception
		{
			String[] syllable3 = { "TIN", "NICKEL", "BRASS", "STEEL", "COPPER" };
			String p_type = syllable3[generateRandomInteger( 0, 4)];
			return p_type;
		}

		String generateWord1() throws Exception
		{
			String[] words = { "special", "pending", "unusual", "express" };
			String word = words[generateRandomInteger( 0, 3)];
			return word;
		}

		String generateWord2() throws Exception
		{
			String[] words = { "packages", "requests", "accounts", "deposits" };
			String word = words[generateRandomInteger( 0, 3)];
			return word;
		}

	//----------------------------------------------------------------------------------------------------------

	void start() {
		// Initialize Random Number Generator
		Long generatorSeed = (long) 0 ;
		if (option_controlrandom) {
			generatorSeed = (long) ((stream_number + 1) * 1234567) ;
		} else {
			generatorSeed = (long) ((stream_number + 1) * 1234567 + System.currentTimeMillis());
		}
		logger.trace(stream_number + "> generatorSeed : " + generatorSeed);

		generator.setSeed( generatorSeed );

	}

	//----------------------------------------------------------------------------------------------------------

	void close() {

	}
}