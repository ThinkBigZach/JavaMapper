package com.chs.utils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class TDConnector {
	
	private static Logger LOG = Logger.getLogger("TDConnector");			
	private static String host;
	private static String user;
	private static String password;
	private static String database;
	private static Connection conn;
	
	private TDConnector() {/* Empty private constructor just because */}
	
	/**
	 * Starts Teradata Connection; must call init() before attempting to query/connect
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
					LOG.fatal("Connection to Teradata failed.");
					System.out.println("returnCode=FAILURE");
				}
			} catch (Exception e)
			{
				LOG.fatal("Connection to Teradata could not be established: " + e.getMessage());
				System.out.println("returnCode=FAILURE");
			}
		}
		return conn;
	}
	
	/**
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

	public static Map<String, Integer> getColumnCounts() throws SQLException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String describeTable = "DESCRIBE dbc.tablesV";
		String query =
				"SELECT c.tablename, count(*) FROM dbc.columnsV c JOIN dbc.tablesV t ON c.databasename = t.databasename AND c.tablename = t.tablename WHERE UPPER(t.databasename) = 'dbc' AND t.commentstring NOT IN ('Ignore') AND c.commentstring NOT IN ('Ignore','ETL') GROUP BY c.tablename";
//		System.out.println("EXECUTING QUERY" + query);
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
	
	/**
	 * Queries Teradata for schemas regarding entities available (MUST call init() and getConnection() FIRST)
	 * @return Map populated with schemas from Teradata for available entities
	 */
	public static Map<String, List<SchemaRecord>> getSchemas()
	{
		Map<String, List<SchemaRecord>> schemaInfo = null;
		try {
			schemaInfo = new LinkedHashMap<String, List<SchemaRecord>>();//c.columnname
			String query = String.format("SELECT c.tablename, c.columntitle, c.columnid, c.commentstring FROM dbc.columnsV c WHERE c.databasename = '%s' and lower(tablename) = tablename (casespecific) and coalesce(commentstring,'') not in ('Ignore','ETL') and coalesce(columntitle,'') not in ('') order by 1,3", database);

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
				if (schemaInfo.containsKey(set.getString(1).toLowerCase()))
				{
					schemaInfo.get(set.getString(1).toLowerCase()).add(new SchemaRecord(fixedTitle, set.getString(3), set.getString(4)));
				}
				else
				{
					SchemaRecord schema = new SchemaRecord(fixedTitle, set.getString(3), set.getString(4));
					List<SchemaRecord> schemarec = new ArrayList<SchemaRecord>();
					schemarec.add(schema);
					schemaInfo.put(set.getString(1).toLowerCase(), schemarec);
				}
				set.next();
			}
		} catch (Exception e) {
			LOG.fatal("Schemas could not be retrieved: " + e.getMessage());
			System.out.println("returnCode=FAILURE");
		}
		return schemaInfo;
	}

	/**
	 * Executes given SQL query
	 * @param sql
	 * @throws SQLException
	 */
	public void executeQuery(String sql) throws SQLException
	{
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		ResultSet set = stmt.executeQuery(sql);
	}
}
