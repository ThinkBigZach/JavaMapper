package com.chs.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class TDConnector {

	private String host;
	private String user;
	private String password;
	private String database;
	
	public TDConnector(String _host, String _user, String _password, String _database) {
		host=_host;
		user=_user;
		password=_password;
		database=_database;
	}
	
	public void Connect()
	{
		Connection conn;
		try
		{
			Class.forName("com.teradata.jdbc.TeraDriver").newInstance();
			String url = "jdbc:teradata//" + host + "/DATABASE=" + database;
			conn = DriverManager.getConnection(url, user, password); //Connection w/user and password
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
