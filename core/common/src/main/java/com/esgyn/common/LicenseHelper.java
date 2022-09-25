// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2017-2019 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package com.esgyn.common;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.Days;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Helper class to interpret EsgynDB license 
 *
 */
public class LicenseHelper {

	private static final Logger _LOG = LoggerFactory.getLogger(LicenseHelper.class);
	private static String LICENSE_FILENAME = "/etc/trafodion/esgyndb_license";
    private static long lastCheckDate = 0;
    private static JsonNode licenseJsonNode = null;
    
    static void loadLicenseDetails() {
        String licenseFileName = System.getenv("SQ_MON_LICENSE_FILE");
        if (licenseFileName == null || licenseFileName.isEmpty())
            licenseFileName = LICENSE_FILENAME;
        try{
            File f = new File(licenseFileName);
            if(licenseJsonNode == null || (f.exists() && !f.isDirectory() && f.lastModified() > lastCheckDate)) {
                lastCheckDate = f.lastModified();
                
                //If the license file exists, invoke the decoder and extract the license details
                List<String> args = new ArrayList<String>();
                args.addAll(Arrays.asList("decoder", "-j", "-f", licenseFileName));
                String output = CommonHelper.runShellCommand(args);
                String[] lines = output.split(System.getProperty("line.separator"));
                if(lines.length > 0) {
                    JsonFactory factory = new JsonFactory();
                    ObjectMapper mapper = new ObjectMapper(factory);
                    licenseJsonNode = mapper.readTree(lines[0].trim());
                }
            }
        }catch(Exception ex){
            _LOG.error("Error loading license details : " + ex.getMessage());
        }   
    }
	
	//Features Bitmap
	public enum Features
	{
		ADVANCED(1), ENTERPRISE(2), MULTITENANCY(4);

		private int val;

		Features(int val) {
			this.val = val;
		}

		public int getValue() {
			return val;
		}
	}
	/**
	 * Indicates if the EsgynDB license allows Multi-tenancy features 
	 * @return true if feature is enabled, else false
	 */
	public static boolean isMultiTenancyEnabled(){
		return isFeatureEnabled(Features.MULTITENANCY);
	}
	/**
	 * Indicates if the EsgynDB license is for Advanced Edition 
	 * @return true if feature is enabled, else false
	 */
	public static boolean isAdvancedEdition(){
		return isFeatureEnabled(Features.ADVANCED);
	}
	/**
	 * Indicates if the EsgynDB license is for Enterprise Edition 
	 * @return true if feature is enabled, else false
	 */
	public static boolean isEnterpriseEdition(){
		return isFeatureEnabled(Features.ENTERPRISE);
	}

	/**
	 * Helper method to check if a particular feature is enabled in the license
	 * @param Feature enum 
	 * @return true if feature is enabled, else false
	 */
	private static boolean isFeatureEnabled(Features feature){
        boolean featureEnabled = false;
        int featureBitmap = -1;
        try{
            loadLicenseDetails();
            if (licenseJsonNode != null) {
                featureBitmap = licenseJsonNode.get("feature_bits").intValue();              
            }
            featureEnabled = featureBitmap >= 0 && ((featureBitmap & feature.getValue()) != 0);
        }catch(Exception ex){
            _LOG.error(ex.getMessage());
         }   
        return featureEnabled;
    }	
    //Modules Bitmap
    public enum Modules
    {
        ROW_LOCK(1), DDL_ONLINE(2),TRIGGER(3),STORED_PROCEDURE(4),CONNECT_BY(5),SEC_AUDIT(6),SEC_MAC(7),LOCAL_AUTH(8),
        LITTLE_SHEET_CACHE(9),HIVE(10),MULTITENANCY(11),BACKUP_RESTORE(12),BINLOG(13),LOB(14),MAX_MODULE(15);

        private int val;

        Modules(int val) {
            this.val = val;
        }

        public int getValue() {
            return val;
        }
    }

    /**
     * Helper method to check if a particular module is enabled in the license
     * @param Module enum 
     * @return true if module is enabled, else false
     */
    public static boolean isModuleOpen(Modules module){
        boolean moduleEnabled = false;
        int moduleId = module.getValue();
        if(moduleId < 1 || moduleId >= Modules.MAX_MODULE.getValue()){
            return false;
        }
        String  moduleBitmap = "";
        try{
            loadLicenseDetails();
            if (licenseJsonNode != null) {
                moduleBitmap = licenseJsonNode.get("Module Code").textValue();
            }
            if((!moduleBitmap.equals(""))){
                String[] sValue = moduleBitmap.split("\\?");
                if(0 == moduleId%32){
                    if(sValue.length <= ((moduleId/32)-1)){
                        return false;
                    }
                    int tmpId = Integer.parseInt(sValue[(moduleId/32)-1]);
                    moduleEnabled = ((1<<(32-1)) & tmpId)>0?true:false;
                }else{
                    if(sValue.length <= (moduleId/32)){
                        return false;
                    }
                    int tmpId = Integer.parseInt(sValue[(moduleId/32)]);
                    moduleEnabled = (1<<(((moduleId % 32)-1))& tmpId)>0?true:false;
                }
            }	
        }catch(Exception ex){
            _LOG.error(ex.getMessage());
        } 
        return moduleEnabled;
    }

	/**
	 * Helper method to get license expiration days since epoch
	 * @return days since epoch
	 */
	public static int getExpiryDays(){
	   int expiryDays = -9999999;
	   try{
           loadLicenseDetails();
           if (licenseJsonNode != null) {
               expiryDays = licenseJsonNode.get("expiry_days").intValue();              
           }
       }catch(Exception ex){
           _LOG.error(ex.getMessage());
       }   
       return expiryDays;	   
	}
	
	/**
	 * Helper method to get the number of nodes licensed to run EsgynDB
	 * @return number of licensed nodes
	 */
	public static int getLicensedNodesCount(){
	    int licensedNodesCount = -1;
        try{
            loadLicenseDetails();
            if (licenseJsonNode != null) {
                licensedNodesCount = licenseJsonNode.get("licensed_nodes").intValue();              
            }
        }catch(Exception ex){
            _LOG.error(ex.getMessage());
        }	    
		return licensedNodesCount;
	}
	
	/**
	 * Helper method to get the license type
	 * @return the license type
	 */
	public static String getLicenseType(){
	    String licenseType = "";
        try{
            loadLicenseDetails();
            if (licenseJsonNode != null) {
                licenseType = licenseJsonNode.get("license_type").textValue();              
            }
         }catch(Exception ex){
            _LOG.error(ex.getMessage());
        }
		return licenseType;
	}
	
    public static String getProductVersion() {
        String productVersion = System.getenv("ESGYN_PRODUCT_VER");
        return productVersion;
    }
    
    /**
     * Helper method to get the product edition
     * @return the product edition
     */
    public static String getProductEdition(){
       String productEdition = "";
       try{
           loadLicenseDetails();
           if (licenseJsonNode != null) {
              if(licenseJsonNode.has("licensed_product"))
                      productEdition = licenseJsonNode.get("licensed_product").textValue();
           }
        }catch(Exception ex){
           _LOG.error("Failed to get licened product info : " + ex.getMessage());
        }
        return productEdition;
    }
    
    public static String getDatabaseEdition() {
    	String databaseEdition = "";
        try{
            loadLicenseDetails();
            if (licenseJsonNode != null) {
            	if(licenseJsonNode.has("licensed_package"))
            		databaseEdition = licenseJsonNode.get("licensed_package").textValue();              
            }
         }catch(Exception ex){
            _LOG.error("Failed to get licened db edition : " + ex.getMessage());
        }
        return databaseEdition;    	
    }
    
	public static void main(String[] args)
    {
    	if(args.length > 0){
    		LicenseHelper.LICENSE_FILENAME = args[0];
    	}
        System.out.println("Product Edition : " + LicenseHelper.getProductEdition() + " " + LicenseHelper.getProductVersion());
        System.out.println("Advanced Edition " + LicenseHelper.isAdvancedEdition());
        System.out.println("Enterprise Edition " + LicenseHelper.isEnterpriseEdition());
    	System.out.println("Licensed Nodes : " + LicenseHelper.getLicensedNodesCount());
        System.out.println("License Type : " + LicenseHelper.getLicenseType());
           	
    	int expiryDays = LicenseHelper.getExpiryDays();
    	try {
            MutableDateTime epoch = new MutableDateTime();
            epoch.setDate(0); //Set to Epoch time
            epoch.setTime(0);
            
            Days days = Days.daysBetween(epoch, new MutableDateTime());
            int expD =  expiryDays - days.getDays();
            epoch.addDays(expiryDays);
            DateTimeFormatter fmt = DateTimeFormat.forPattern("YYYY-MM-dd");
            System.out.println("License expiry date : " + fmt.print(epoch));
            if(expD > 0) {
            	System.out.println("License will expire in " + expD + " days");
            }else {
            	System.out.println("License has expired since " + expD + " days");
            }
        } catch (Exception e) {
            _LOG.error(e.getMessage());
        }
        System.out.println("MultiTenancy Feature " + LicenseHelper.isMultiTenancyEnabled());
    	
    }
}
