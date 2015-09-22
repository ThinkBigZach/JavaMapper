package com.chs.utils;

public class SchemaRecord 
{
	private String column_name;
	private String column_id;

	public String getColumn_name() {
		return column_name;
	}

	public String getColumn_id() {
		return column_id;
	}
	
	public SchemaRecord(String columnName)
	{
		column_name = columnName;
	}

	public SchemaRecord(String columnName, String columnType)
	{
		column_name = columnName;
		column_id = columnType;
	}
	
	
}
