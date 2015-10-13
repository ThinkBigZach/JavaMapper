package com.chs.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.Map.Entry;

public class SchemaMatcher {
	
	private static String delimiter = "\036";
	private static String spacelimiter = "\037";
	private static Logger LOG = Logger.getLogger("SchemaMatcher");
	public static Map<String, List<SchemaRecord>> goldenEntitySchemaMap = TDConnector.getSchemas();
	
	/**
	 * Dynamic Schema Matcher used to match against a Golden Schema for a given entity
	 * @param entity 
	 * @param compareURL
	 * @param fs
	 * @return true if the schema from compareURL file matches golden schema, false otherwise
	 * @throws FileNotFoundException
	 */
    public static boolean matchSchemas(String entity, String compareURL, FileSystem fs) throws FileNotFoundException {
        boolean tripwire = false;
        Map<String, String> goldenMap = getGoldenSchema(entity.toLowerCase());
        Map<String, String> compareMap = null;
        Scanner compareFile = null;
        String line1 = null;
        try {
	        compareFile = new Scanner(fs.open(new Path(compareURL))).useDelimiter(delimiter);
	        compareMap = extractMapFromFile(cleanStringByColumn(compareFile.next()), cleanStringByColumn(compareFile.next()));
		} catch (Exception e) {
			LOG.fatal("SCHEMA COULD NOT BE MATCHED; " + e.getMessage());
			System.out.println("returnCode=FAILURE");
		}
        if ((goldenMap != null && compareMap != null))//compareMap.length >= goldenMap.length
        {
        	if (schemaMatch(goldenMap, compareMap, goldenMap.size(), entity))
        	{
        		//Successful match
        		tripwire = true;
        	}
        	else
        	{
        		//Failed to match
        		LOG.warn("SCHEMA NOT MATCHED FOR ENTITY " + entity);
        	}
        } else {
        	LOG.warn("SCHEMA COULD NOT BE MATCHED");
        }
        compareFile.close();
        return tripwire;
    }
    
    public static Map<String, Integer> getOrderingSchema(String entity)
    {
    	Map<String, Integer> tempmap = new LinkedHashMap<String, Integer>();
    	List<SchemaRecord> recordList = goldenEntitySchemaMap.get(entity);
    	if(recordList != null)
    	{
    		int count = 0;
    		for (SchemaRecord sr : recordList)
    		{
    			tempmap.put(sr.getColumn_name().toLowerCase(), count);
    			count++;
    		}
    	}
    	else
    	{
    		LOG.warn(String.format("GOLDEN SCHEMA FOR ENTITY %s IS NULL", entity));
    	}
    	return tempmap;
    }
    
    /**
     * Retrieves golden schema for given entity
     * @param entity
     * @return Map containing golden schema for entity
     */
    public static Map<String, String> getGoldenSchema(String entity)
    {
    	Map<String,String> tempmap = new HashMap<String,String>();
    	List<SchemaRecord> recordList = goldenEntitySchemaMap.get(entity);
    	if(recordList != null)
    	{
    		for (SchemaRecord sr : recordList)
    		{
    			tempmap.put(sr.getColumn_name(), sr.getColumn_id());
    		}
    	}
    	return tempmap;
    }
    
    /**
     * Schema matching logic, matches column titles against golden copy
     * @param file1Map
     * @param file2map
     * @param mapSize
     * @param entity
     * @return true if every column title from file matches a title in the golden schema, false otherwise
     */
    private static boolean schemaMatch(Map<String,String> file1Map, Map<String,String> file2map, int mapSize, String entity)
	{
		int ticket = 0;
		for(Entry<String,String> kv : file2map.entrySet())
		{
			String mapkey = kv.getKey().toLowerCase().replaceAll(" ", "_");
			if (file1Map.containsKey(mapkey))
			{
				//Match
				ticket++;
			}
			else
			{
				//No Match
			}
		}
		return (ticket >= mapSize);
	}

    private static String[] cleanStringByColumn(String beCleaned) {
        return beCleaned.replaceAll(delimiter, "")
                .replaceAll("\n", "")
                .replaceAll(spacelimiter, ",").split(",");
    }
    
    /**
     * Creates map-schema from file
     * @param column_Names
     * @param column_Types
     * @return Map-schema of file 
     */
    private static Map<String,String> extractMapFromFile(String[] column_Names, String[] column_Types)
	{
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < column_Names.length; i++)
		{
			map.put(column_Names[i], column_Types[i]);
		}
		return map;
	}
}