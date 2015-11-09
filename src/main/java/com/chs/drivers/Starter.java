package com.chs.drivers;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.net.SyslogAppender;
import org.apache.log4j.Logger;

public class Starter {

	private static Logger LOG = Logger.getLogger("Starter");


	//args[0] -- INPUT PATH LIKE THIS - /user/financialDataFeed/data/*/finished
	//args[1] -- entity like this - allergy or "*" "*" will give you all entities
	//args[2] -- date for which it is run over yyyy-mm-dd[-yyyy-mm-dd]
	//args[3] -- outpath like this -- /user/athena/financialdatafeed/extracted/finished
	//args[4] -- valid practice map location like this --/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv
	//args[5] -- valid entity map location like this -- /enterprise/mappings/athena/athena_table_defs.csv
	//args[6] -- valid division map location like -- /enterprise/mappings/athena/chs-division-id-mapping-athena.csv
	//TD_HOST - args[7] -dev.teradata.chs.net
	//TD_USER - args[8] - dbc
	//TD_PSWD - args[9] - dbc
	//TD_DATABASE - args[10] -- EDW_ATHENA_STAGE
	//divisional OR path as args[11]
	//validate OR standard as args[12]
	public static void main(String[] args) throws Exception {
//		PropertyConfigurator.configure("/src/log4j.properties");
		if(args.length != 13) {
			System.out.println("returnCode=FAILURE");
			LOG.info("Expected 13 parameters, received " + args.length);
			throw new Exception("Expected 13 parameters, received " + args.length);
		}
		Driver driver;
		if(args[11].equals("divisional")) {
			driver = new DivisionalDriver(args);
		}
		else if (args[11].equals("path")) {
			driver = new PathDriver(args);
		}
		else {
			System.out.println("returnCode=FAILURE");
			LOG.info("did not specify \"divisional\" or \"path\" load");
			throw new Exception("you did not specify \"divisional\" or \"path\" load");
		}
		LOG.info("LOG -> JAR INITIALIZED; PROGRAM STARTING.");
		driver.start();
	}

}
