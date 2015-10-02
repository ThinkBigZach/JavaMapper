package com.chs.drivers;

public class Starter {



	//args[0] -- INPUT PATH LIKE THIS - /user/financialDataFeed/data/*/finished/yyyy-mm-dd[-yyyy-mm-dd]
	//args[1] -- entity like this - allergy or "" "" will give you all entities
	//args[2] -- outpath like this -- /user/athena/financialdatafeed/extracted/finished
	//args[3] -- valid practice map location like this --/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv
	//args[4] -- valid entity map location like this -- /enterprise/mappings/athena/athena_table_defs.csv
	//TD_HOST - args[5] -dev.teradata.chs.net
	//TD_USER - args[6] - dbc
	//TD_PSWD - args[7] - dbc
	//TD_DATABASE - args[8] -- EDW_ATHENA_STAGE
	//divisional OR path as args[9]
	public static void main(String[] args) throws Exception {
		if(args.length != 11) {
			System.out.println("returnCode=FAILURE");
			throw new Exception("Expected 10 parameters, received " + args.length);
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
			throw new Exception("you did not specify \"divisional\" or \"path\" load");
		}
		
		driver.start();
	}

}
