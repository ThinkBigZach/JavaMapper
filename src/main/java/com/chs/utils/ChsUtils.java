package com.chs.utils;

import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map.Entry;

public class ChsUtils {
    public static final String CR = "\r"; //carriage return
    public static final String LF = "\n"; //line feed
    public static final String UNIT_SEPARATOR = "\037";
    public static final String RECORD_SEPARATOR = "\036";
    
    public static String replaceCRandLF(String line){
        line = line.replaceAll(CR, " ");
        line = line.replaceAll(LF, "");
        line = line.replaceAll(RECORD_SEPARATOR, "");
        return line;
    }


    public static void getValidPracticeIds(String path, ArrayList<String> validPracticeIDs, FileSystem fs) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice.toUpperCase());
        }
    }

    public static void getValidEntityNames(String pathToMap, String out_path, ArrayList<String> validEntityNames, FileSystem fs) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToMap))));
        String line = "";
        while((line = br.readLine()) != null){
            String currentValidName = line.toUpperCase();
            String tempWriteDir = out_path +"/" + currentValidName.toLowerCase() + "/";
            if (!fs.exists(new Path(tempWriteDir))) {
                fs.mkdirs(new Path(tempWriteDir));
            }
            validEntityNames.add(currentValidName);
        }
    }
    public static String getPatternMatch(String header){
        String[] headerInfo = header.split(UNIT_SEPARATOR);
        String varcharMatch = ".*";
        String numberMatch = "[+-]?(\\d+\\.?\\d*)";
        String pattern = "";
        for(String s : headerInfo){
            if(s.equalsIgnoreCase("NUMBER")){
                    pattern += numberMatch + UNIT_SEPARATOR;
            }
            else{
                pattern += varcharMatch + UNIT_SEPARATOR;
            }
        }
        pattern = pattern.substring(0, pattern.lastIndexOf(UNIT_SEPARATOR));
        System.out.println(pattern);
        return pattern;
    }
    public static String appendTimeAndExtension(String s) {

        String time = "."+System.currentTimeMillis();
        return s += time + ".txt";

    }
    
    public static ArrayList<Integer> getNumberIndices(String header){
        String[] line = header.split(UNIT_SEPARATOR);
        ArrayList<Integer> arr = new ArrayList<Integer>();
        int index = 0;
        for(String s : line){
            if(s.equalsIgnoreCase("NUMBER")){
                arr.add(index);
            }
            index++;
        }
        return arr;
    }

    public static boolean matchNumberTypes(String line, ArrayList<Integer> numberCols){
        String[] matchLine = line.split(UNIT_SEPARATOR);

        for(Integer i : numberCols){
            try {
                if (!matchLine[i].equals("") && matchLine[i].contains(".")) {
                    try {
                        double d = Double.parseDouble(matchLine[i].trim());
                    } catch (NumberFormatException e) {
                        return false;
                    }
                } else if(!matchLine[i].equals("")) {
                    try {
                        int index = Integer.parseInt(matchLine[i].trim());
                    } catch (NumberFormatException e) {
                        return false;
                    }
                }
            }
            catch(IndexOutOfBoundsException e){
                //Catches the nullpointer to make sure the data still writes if null
            }
            catch(NullPointerException e){

            }
            catch(Exception e){
                return false;
            }
        }
        return true;
    }
    
    public static boolean needsDynamicSchemaReorder(Map<String,Integer> goldSchema, String headerinfo)
    {
    	headerinfo = headerinfo.replace(" ", "_");
    	String goldSchemaHead = goldSchema.keySet().toString().replaceAll(", ", UNIT_SEPARATOR);
    	goldSchemaHead = goldSchemaHead.substring(1, goldSchemaHead.length()-1);
//    	System.out.println(headerinfo + "\n" + goldSchemaHead);
    	return !headerinfo.equalsIgnoreCase(goldSchemaHead);
//    	for(int i = 0; i < headerinfo.length; i++)
//    	{
//    		String cleanhead = headerinfo[i].replace(" ", "_").toLowerCase();
//    		if (goldSchema.get(cleanhead) == null || goldSchema.get(cleanhead) != i)
//    		{
//    			return true;
//    		}
//    	}
//    	return false;
    }
    
    public static String reorderAlongSchema(Map<String, Integer> goldSchema, String[] schemaColumns, String[] headerinfo)
    {
    	StringBuilder orderedScheme = new StringBuilder();
    	Map<Integer,String> columnMap = new HashMap<Integer,String>();
    	Map<String,Integer> headerMap = new HashMap<String,Integer>();
//    	System.out.println(String.format("File column size: %s \n\tHeader column size: %s", schemaColumns.length, headerinfo.length));
    	for (int i = 0; i < schemaColumns.length; i++)
     	{
    		columnMap.put(i, schemaColumns[i]);
    		headerMap.put(headerinfo[i].replaceAll(" ", "_").toLowerCase(), i);
     	}
    	for (Entry<String,Integer> kv : goldSchema.entrySet())
     	{
//    		System.out.println("GOLD KEY: " + kv.getKey());
    		if (headerMap.containsKey(kv.getKey()))
    		{
    			int goldCol = headerMap.remove(kv.getKey());//headerMap.get(kv.getKey());
    			String colValue = columnMap.get(goldCol);
    			orderedScheme.append(colValue).append(UNIT_SEPARATOR);    			
    		}
     	}
    	if(!headerMap.isEmpty())
    	{
    		System.out.println("returnCode=FAILURE");
    	}
    	String schemaReorder = orderedScheme.substring(0, (orderedScheme.length() - 1));
    	return schemaReorder;
    }
 
    public static boolean validateColumnCounts(String entity, String colFile2, FileSystem fs) throws FileNotFoundException{
    	return SchemaMatcher.matchSchemas(entity, colFile2, fs);
    }
    
}
