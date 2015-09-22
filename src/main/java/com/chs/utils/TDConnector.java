package com.chs.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
	
	public static Map<String, List<SchemaRecord>> getSchemas()
	{
		Map<String, List<SchemaRecord>> schemaInfo = null;
		try {
			schemaInfo = new HashMap<String, List<SchemaRecord>>();//c.columnname
			String query = "SELECT c.tablename, c.columntitle, c.columnid FROM dbc.columnsV c WHERE c.databasename = 'EDW_ATHENA_STAGE' and lower(tablename) = tablename (casespecific) and coalesce(commentstring,'') not in ('Ignore','ETL') and coalesce(columntitle,'') not in ('') order by 1,3";
			Connection conn = getConnection();
			Statement stmt = conn.createStatement();
			ResultSet set = stmt.executeQuery(query);
			while (set.isBeforeFirst())
			{
				set.next();
			}
			while (!set.isAfterLast())
			{
				String fixedTitle = set.getString(2).replace(" ", "_").toLowerCase();
				//System.out.println("Tablename: " + set.getString(1) + "\n\tColumn title: " + set.getString(2) + "\n\tColumn type: " + set.getString(3));
				if (schemaInfo.containsKey(set.getString(1).toLowerCase()))
				{
					schemaInfo.get(set.getString(1).toLowerCase()).add(new SchemaRecord(fixedTitle, set.getString(3)));
				}
				else
				{
					String types = set.getString(3);
					
					SchemaRecord schema = new SchemaRecord(fixedTitle, set.getString(3));
					List<SchemaRecord> schemarec = new ArrayList<SchemaRecord>();
					schemarec.add(schema);
					schemaInfo.put(set.getString(1).toLowerCase(), schemarec);
				}
				set.next();
			}
		} catch (Exception e) {
			e.printStackTrace();
			//throw e;
		}
		return schemaInfo;
	}
	
	private void translate(String key)
	{
		String pay = new String();
		if(key.equalsIgnoreCase("I"))
		{
			pay = "INT";
		}
		else if (key.equalsIgnoreCase("N"))
		{
			pay = "DOUBLE";
		}
		else
		{
			pay = "STRING";
		}
	}

	public void executeQuery(String sql) throws SQLException
	{
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		ResultSet set = stmt.executeQuery(sql);



	}
}
