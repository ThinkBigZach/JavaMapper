package com.chs.drivers;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.net.SyslogAppender;
import org.apache.log4j.Logger;

public class Starter {

	private static Logger LOG = Logger.getLogger("Starter");


	//args[0] -- INPUT PATH LIKE THIS - /user/financialDataFeed/data/*/finished/yyyy-mm-dd[-yyyy-mm-dd]
	//args[1] -- entity like this - allergy or "" "" will give you all entities
	//args[2] -- outpath like this -- /user/athena/financialdatafeed/extracted/finished
	//args[3] -- valid practice map location like this --/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv
	//args[4] -- valid entity map location like this -- /enterprise/mappings/athena/athena_table_defs.csv
	//args[5] -- valid division map location like -- /enterprise/mappings/athena/chs-division-id-mapping-athena.csv
	//TD_HOST - args[6] -dev.teradata.chs.net
	//TD_USER - args[7] - dbc
	//TD_PSWD - args[8] - dbc
	//TD_DATABASE - args[9] -- EDW_ATHENA_STAGE
	//divisional OR path as args[10]
	//verbose OR standard as args[11]
	public static void main(String[] args) throws Exception {
//		BasicConfigurator.configure(new SyslogAppender());
		PropertyConfigurator.configure("/src/log4j.properties");
		if(args.length != 12) {
			System.out.println("returnCode=FAILURE");
			LOG.info("Expected 12 parameters, received " + args.length);
			throw new Exception("Expected 12 parameters, received " + args.length);
		}
		Driver driver;
		if(args[10].equals("divisional")) {
			driver = new DivisionalDriver(args);
		}
		else if (args[10].equals("path")) {
			driver = new PathDriver(args);
		}
		else {
			System.out.println("returnCode=FAILURE");
			LOG.info("did not specify \"divisional\" or \"path\" load");
			throw new Exception("you did not specify \"divisional\" or \"path\" load");
		}
		LOG.info("LOG -> JAR INITIALIZED; PROGRAM STARTING.");
		System.out.println("CONSOLE -> JAR INITIALIZED; PROGRAM STARTING");
		driver.start();
	}

}
