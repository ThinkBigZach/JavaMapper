package com.chs.utils;

import java.util.List;

public class PiiObfuscator {

	/**
	 * This processes each line if it has personally identifiable info which is indicated
	 * by a comment_string equal to remove
	 * @param line - The line to be processed
	 * @param headerInfo - the header for that entity
	 * @param schemarecords - the schema records for that entity
	 * @param delimiter - the delimiter that line is delimited by (/036 is the regular one)
	 * @return
	 */
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


	/**
	 * For a given list of schema records, this will return true if that entity has personally identifiable information
	 * Otherwise it'll return false.
	 * @param records
	 * @return
	 */
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
