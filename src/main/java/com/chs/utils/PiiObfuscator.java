package com.chs.utils;

import java.util.List;

public class PiiObfuscator {

	
	public static String piiProcess(String[] line, String[] headerInfo, List<SchemaRecord> schemarecords, String delimiter) 
    {
    	StringBuilder builder = new StringBuilder();
		for(int i = 0; i < line.length; i++)
		{
			for(SchemaRecord sr : schemarecords)
			{
				if (sr.getColumn_name().equalsIgnoreCase(headerInfo[i].replaceAll(" ", "_")))
				{
					if (sr.getCommentstring() != null && sr.getCommentstring().equalsIgnoreCase("remove"))
					{
						builder.append("" + delimiter);
					}
					else
					{
						builder.append(line[i]).append(delimiter);
					}
				}
			}
		}
        String val = builder.toString();
        val = val.substring(0, val.lastIndexOf(delimiter));
		return val;
	}

	public static boolean hasRemoveComment(List<SchemaRecord> records)
    {
    	for(SchemaRecord r : records)
    	{
    		if (r.getCommentstring() != null && r.getCommentstring().equalsIgnoreCase("remove"))
    		{
    			return true;
    		}
    	}
    	return false;
    }
}
