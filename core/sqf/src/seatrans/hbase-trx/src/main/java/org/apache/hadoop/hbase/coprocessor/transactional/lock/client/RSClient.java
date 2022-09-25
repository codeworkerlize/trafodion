package com.esgyn.rs;

import org.apache.log4j.BasicConfigurator;

public class RSClient {
    public static void main(String[] args) {
    	BasicConfigurator.configure();
        RSciCommand rsciCommand = new RSciCommand();
        rsciCommand.commandDeal(args);
        return;
    }
}
    
