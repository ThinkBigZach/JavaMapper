package drivers;

public class Starter {

	public static void main(String[] args) throws Exception {
		if(args.length != 10) {
			throw new Exception("Expected 10 parameters, recieved " + args.length);
		}
		Driver driver;
		if(args[9].equals("divisional")) {
			driver = new DivisionalDriver();
		}
		else if (args[9].equals("path")) {
			driver = new PathDriver();
		}
		else {
			throw new Exception("you did not specify \"divisional\" or \"path\" load");
		}
		
		driver.start(args);
		
	}

}
