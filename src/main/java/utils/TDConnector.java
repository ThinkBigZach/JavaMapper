package utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class TDConnector {

	public void Connect()
	{
		Connection conn;
		try
		{
			Class.forName("com.teradata.jdbc.TeraDriver").newInstance();
			String url = "jdbc:teradata//dev.teradata.chs.net/DATABASE=EDW_ATHENA_STAGE";
			conn = DriverManager.getConnection(url, "dbc", "dbc"); //Connection w/user and password
			if (conn.isValid(0))
			{
				System.out.println("Connected to Teradata");
			}
			else
			{
				System.out.println("Connection to Teradata failed");
			}
		} catch (Exception e)
		{
			System.out.println("Exception caught: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
