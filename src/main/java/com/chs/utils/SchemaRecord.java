package com.chs.utils;

/**
 * Class that holds the column names, titles, and comment string from the Teradata database
 */
public class SchemaRecord 
{
	private String column_name;
	private String column_id;

	//If comment string is equal to remove, then that column is personally identifiable.
	private String commentstring;

	public String getColumn_name() {
		return column_name;
	}

	public String getColumn_id() {
		return column_id;
	}
	
	public String getCommentstring()
	{
		return commentstring;
	}
	
	public SchemaRecord(String columnName)
	{
		column_name = columnName;
	}

	public SchemaRecord(String columnName, String columnType, String commentString)
	{
		column_name = columnName;
		column_id = columnType;
		commentstring = commentString;
	}
	
	
}
