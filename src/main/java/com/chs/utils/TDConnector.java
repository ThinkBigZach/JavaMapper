package com.chs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TDConnector {

	private static String host;
	private static String user;
	private static String password;
	private static String database;
	private static Connection conn;
	
	private TDConnector() {/* Empty private constructor just because */}
	
	/***
	 * Must call init() before attempting to query/connect
	 * @return
	 */
	public static Connection getConnection()
	{
		if (conn == null)
		{
			try
			{
				Class.forName("com.teradata.jdbc.TeraDriver").newInstance();
				String url = "jdbc:teradata//" + host + "/DATABASE=" + database;
				conn = DriverManager.getConnection(url, user, password); //Connection w/user and password
				if (!conn.isValid(0))
				{
					throw new Exception("Connection to Teradata failed..");
				}
			} catch (Exception e)
			{
				System.out.println("Exception caught: " + e.getMessage());
				e.printStackTrace();
			}
		}
		return conn;
	}
	
	/***
	 * Sets the connection parameters
	 * @param _host
	 * @param _user
	 * @param _password
	 * @param _database
	 */
	public static void init(String _host, String _user, String _password, String _database) {
		host=_host;
		user=_user;
		password=_password;
		database=_database;
	}
	
	public void executeQuery(String sql) throws SQLException
	{
		Connection con = getConnection();
		Statement stmt = conn.createStatement();
		stmt.executeQuery(sql);
	}
}
