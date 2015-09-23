package com.chs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author cr186034
 */

//TODO(1): Put file(path) compareURL into "error array"
public class SchemaMatcher {
	
	private static String delimiter = "\036";
	private static String spacelimiter = "\037";
	private static Logger LOG = Logger.getLogger(SchemaMatcher.class);
	public static Map<String, List<SchemaRecord>> goldenEntitySchemaMap = TDConnector.getSchemas();
	
    public static boolean matchSchemas(String entity, String compareURL, FileSystem fs) throws FileNotFoundException {
        boolean tripwire = false;
        Map<String, String> goldenMap = getGoldenSchema(entity.toLowerCase());
        Map<String, String> compareMap = null;
        Scanner goldenFile = null;
        Scanner compareFile = null;
        String line1 = null;
        //goldenMap = extractMapFromFile(cleanStringByColumn(goldenFile.next()), cleanStringByColumn(goldenFile.next()));
        //compareMap = extractMapFromFile(cleanStringByColumn(compareFile.next()), cleanStringByColumn(compareFile.next()));
        try {
	        //goldenFile = new Scanner(new File(goldenURL)).useDelimiter(delimiter);
	        compareFile = new Scanner(fs.open(new Path(compareURL))).useDelimiter(delimiter);
	        compareMap = extractMapFromFile(cleanStringByColumn(compareFile.next()), cleanStringByColumn(compareFile.next()));
		} catch (Exception e) {
			LOG.info("====SCHEMA COULD NOT BE MATCHED====");
			System.out.println(String.format("CompareFile: %s \nLine: %s", compareURL, line1));
			e.printStackTrace();
		}
        //Dynamic Schema Match change >> Checks to make sure column and column data type are equal. All must match to pass.
        if ((goldenMap != null && compareMap != null)) //&& goldenMap.size() == compareMap.size()) 
        {
        	if (schemaMatch(goldenMap, compareMap, goldenMap.size(), entity))
        	{
        		//LOG.info("==========Schema match success===========");
        		tripwire = true;        		
        	}
        	else
        	{
        		LOG.info("==========Schema match failed============");        		
        		//TODO:1        	
        	}
        } else {
        	LOG.info("====SCHEMA COULD NOT BE MATCHED");
        	//System.out.println(String.format("Golden map: %s<->%s \nCompareMap: %s<->%s", goldenMap, goldenMap.size(), compareMap, compareMap.size()));
        }
        compareFile.close();
        return tripwire;
    }
    
    public static Map<String, Integer> getOrderingSchema(String entity)
    {
    	Map<String, Integer> tempmap = new HashMap<String, Integer>();
    	List<SchemaRecord> recordList = goldenEntitySchemaMap.get(entity);
    	if(recordList != null)
    	{
    		int count = 0;
    		for (SchemaRecord sr : recordList)
    		{
    			//tempmap.put(sr.getColumn_name(), sr.getColumn_id());
    			tempmap.put(sr.getColumn_name().toLowerCase(), count);
    			//System.out.println(String.format("ADDING -> [%s,%s]", count, sr.getColumn_name()));
    			count++;
    		}
    	}
    	else
    	{
    		System.out.println(String.format("ENTITY %s IS NULL"));
    	}
    	return tempmap;
    }
    
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
    
    private static boolean schemaMatch(Map<String,String> file1Map, Map<String,String> file2map, int mapSize, String entity)
	{
		//Dont forget to return ticket on final
		int ticket = 0;
//		for (Entry<String,String> kv : file1Map.entrySet())
//		{
//			System.out.println("Golden \n\tTable: " + entity + "\n\tColumn name: " + kv.getKey() + "\n\tColumn id: " + kv.getValue());
//		}
		for(Entry<String,String> kv : file2map.entrySet())
		{
			String mapkey = kv.getKey().toLowerCase().replaceAll(" ", "_");
			//Column names match. If column Types match, we got it.
			//System.out.print("Compare " + mapkey + "..");
			if (file1Map.containsKey(mapkey))//&& kv.getValue().equals(file1Map.get(mapkey)))
			{
				ticket++;
				//System.out.println("MATCHED -> " + mapkey);
			}
			else
			{
				//ticket--;
				//System.out.println("MISSED -> NO MATCH");
			}
		}
		//System.out.println(String.format("MATCH COUNT \t -> %s ? %s", ticket, mapSize));
		return (ticket >= mapSize);
	}

    private static String[] cleanStringByColumn(String beCleaned) {
        return beCleaned.replaceAll(delimiter, "")
                .replaceAll("\n", "")
                .replaceAll(spacelimiter, ",").split(",");
    }
    
    private static Map extractMapFromFile(String[] column_Names, String[] column_Types)
	{
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < column_Names.length; i++)
		{
			map.put(column_Names[i], column_Types[i]);
		}
		return map;
	}
}