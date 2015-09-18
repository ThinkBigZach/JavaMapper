package com.chs.utils;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

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
				String url = "jdbc:teradata://" + host + "/DATABASE=" + database;
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

	//TODO CHANGE 'dbc' to the actual database name WHEN GARY GETS BACK TO US
	public static Map<String, Integer> getColumnCounts() throws SQLException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();


		String describeTable = "DESCRIBE dbc.tablesV";
		String query =
				"SELECT c.tablename, count(*) FROM dbc.columnsV c JOIN dbc.tablesV t ON c.databasename = t.databasename AND c.tablename = t.tablename WHERE UPPER(t.databasename) = 'dbc' AND t.commentstring NOT IN ('Ignore') AND c.commentstring NOT IN ('Ignore','ETL') GROUP BY c.tablename";
		System.out.println("EXECUTING QUERY" + query);
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		ResultSet set = stmt.executeQuery(query);
		while(set.isBeforeFirst()){
			set.next();
		}
		while(!set.isAfterLast()){
			map.put(set.getString(1).toUpperCase(), set.getInt(2));
			set.next();
		}
		return map;
	}

	public void executeQuery(String sql) throws SQLException
	{
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		ResultSet set = stmt.executeQuery(sql);



	}
}
