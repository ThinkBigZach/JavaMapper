package com.chs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.log4j.Logger;

/**
 * @author cr186034
 */

//TODO(1): Put file(path) compareURL into "error array"
public class SchemaMatcher {
	
	private static String delimiter = "\\036";
	private static Logger LOG = Logger.getLogger(SchemaMatcher.class);
	
    public static boolean matchSchemas(String goldenURL, String compareURL) throws FileNotFoundException {
        boolean tripwire = false;
        Scanner goldenFile = new Scanner(new File(goldenURL)).useDelimiter(delimiter);
        Scanner compareFile = new Scanner(new File(compareURL)).useDelimiter(delimiter);
        Map<String, String> goldenMap = null;
		Map<String, String> compareMap = null;
		try {
			goldenMap = extractMapFromFile(cleanStringByColumn(goldenFile.next()), cleanStringByColumn(goldenFile.next()));
			compareMap = extractMapFromFile(cleanStringByColumn(compareFile.next()), cleanStringByColumn(compareFile.next()));
		} catch (Exception e) {
			LOG.info("====SCHEMA COULD NOT BE MATCHED -> COLUMN COUNT NOT EQUAL");
			e.printStackTrace();
		}
        //Dynamic Schema Match change >> Checks to make sure column and column data type are equal. All must match to pass.
        if ((goldenMap != null && compareMap != null)
        		&& goldenMap.size() == compareMap.size()) 
        {
        	if (schemaMatch(goldenMap, compareMap, goldenMap.size()))
        	{
        		LOG.info("==========Schema match success===========");
        		tripwire = true;        		
        	}
        	else
        	{
        		LOG.info("==========Schema match failed============");        		
        		//TODO:1        	
        	}
        } else {
        	LOG.info("====SCHEMA COULD NOT BE MATCHED -> COLUMN COUNT NOT EQUAL");
        }
        goldenFile.close();
        compareFile.close();
        return tripwire;
    }
    
    private static boolean schemaMatch(Map<String,String> file1Map, Map<String,String> file2map, int mapSize)
	{
		//Dont forget to return ticket on final
		int ticket = 0;
		for(Entry<String,String> kv : file2map.entrySet())
		{
			String mapkey = kv.getKey();
			//Column names match. If column Types match, we got it.
			if (file1Map.containsKey(mapkey) && kv.getValue().equals(file1Map.get(mapkey)))
			{
				ticket++;
			}
			else
			{
				ticket--;
			}
		}
		return (ticket == mapSize);
	}

    private static String[] cleanStringByColumn(String beCleaned) {
        return beCleaned.replaceAll("\\036", "")
                .replaceAll("\n", "")
                .replaceAll("\\037", ",").split(",");
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