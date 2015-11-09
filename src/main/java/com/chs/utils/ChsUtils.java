package com.chs.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ChsUtils {
	private static Logger LOG = Logger.getLogger("CHS_Utils");
    public static final String CR = "\r"; //carriage return
    public static final String LF = "\n"; //line feed
    public static final String UNIT_SEPARATOR = "\037";
    public static final String RECORD_SEPARATOR = "\036";


    /**
     * Replaces all carriage returns, line feeds, and record separators
     * @param line
     * @return
     */
    public static String replaceCRandLF(String line){
        line = line.replaceAll(CR, " ");
        line = line.replaceAll(LF, "");
        line = line.replaceAll(RECORD_SEPARATOR, "");
        return line;
    }

    /**
     * Reads in valid division ids from path
     * @param path
     * @param validDivisionIDs
     * @param fs
     * @throws IOException
     */
    public static void getValidDivisionIds(String path, ArrayList<String> validDivisionIDs, FileSystem fs) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = "";
        while ((line = br.readLine()) != null) {
            validDivisionIDs.add(line.split("~")[0]);
        }
        br.close();
    }
    /**
     * Reads in the valid practice ids from path
     * @param path -- path to valid practice ids
     * @param validPracticeIDs -- list of valid practices
     * @param fs -- file system to read in file
     * @throws IOException
     */
    public static void getValidPracticeIds(String path, ArrayList<String> validPracticeIDs, FileSystem fs) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice.toUpperCase());
        }
        br.close();
    }

    /**
     * Reads in all the valid entities from pathToMap
     * @param pathToMap --
     * @param out_path -- out path for entities
     * @param validEntityNames -- List of valid entity names
     * @param fs -- file system to read in the map
     * @throws IOException
     */
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
        br.close();
    }

    /**
     * Appends a timestamp to a file name
     * @param s
     * @return
     */
    public static String appendTimeAndExtension(String s) {
        String time = "."+System.currentTimeMillis();
        return s += time + ".txt";
    }


    /**
     * Finds all the indices for NUMBER fields in the header of a given file
     * @param header
     * @return a list of all the indices
     */
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


    /**
     * Returns if a given line matches all the data types from its header
     * @param line
     * @param numberCols
     * @return true if all the data types match, false otherwise
     */
    public static boolean matchNumberTypes(String line, ArrayList<Integer> numberCols){
        String[] matchLine = line.split(UNIT_SEPARATOR);

        for(Integer i : numberCols){
            try {
                if (!matchLine[i].equals("") && matchLine[i].contains(".")) {
                    try {
                        double d = Double.parseDouble(matchLine[i].trim());
                    } catch (NumberFormatException e) {
                        System.out.println(matchLine[i]);
                        return false;
                    }
                } else if(!matchLine[i].equals("")) {
                    try {
                        long index = Long.parseLong(matchLine[i].trim());
                    } catch (NumberFormatException e) {
                        System.out.println(matchLine[i]);
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
            	LOG.warn(e.getMessage());
                return false;
            }
        }
        return true;
    }
    
    /**
     * Determines if file needs reordering
     * @param goldSchema
     * @param headerinfo
     * @return true if needs reordering, false otherwise
     */
    public static boolean needsDynamicSchemaReorder(Map<String,Integer> goldSchema, String headerinfo)
    {
    	headerinfo = headerinfo.replace(" ", "_");
    	String goldSchemaHead = goldSchema.keySet().toString().replaceAll(", ", UNIT_SEPARATOR);
    	goldSchemaHead = goldSchemaHead.substring(1, goldSchemaHead.length()-1);
    	return !headerinfo.equalsIgnoreCase(goldSchemaHead);
    }
    
    /**
     * Reorders file content along gold schema
     * @param goldSchema
     * @param schemaColumns
     * @param headerinfo
     * @return string representing content in corrected order
     */
    public static String reorderAlongSchema(Map<String, Integer> goldSchema, String[] schemaColumns, String[] headerinfo)
    {
    	StringBuilder orderedScheme = new StringBuilder();
    	Map<Integer,String> columnMap = new HashMap<Integer,String>();
    	Map<String,Integer> headerMap = new HashMap<String,Integer>();
    	for (int i = 0; i < schemaColumns.length; i++)
     	{
    		columnMap.put(i, schemaColumns[i]);
    		headerMap.put(headerinfo[i].replaceAll(" ", "_").toLowerCase(), i);
     	}
    	for (Entry<String,Integer> kv : goldSchema.entrySet())
     	{
    		if (headerMap.containsKey(kv.getKey()))
    		{
    			int goldCol = headerMap.remove(kv.getKey());
    			String colValue = columnMap.get(goldCol);
    			orderedScheme.append(colValue).append(UNIT_SEPARATOR);    			
    		}
     	}
    	if(!headerMap.isEmpty())
    	{
    		LOG.fatal("Header contains column title not found in golden copy; reorder not possible");
    		System.out.println("returnCode=FAILURE");
    	}
    	String schemaReorder = orderedScheme.substring(0, (orderedScheme.length() - 1));
    	return schemaReorder;
    }
 
    public static boolean validateColumnCounts(String entity, String colFile2, FileSystem fs) throws FileNotFoundException{
    	return SchemaMatcher.matchSchemas(entity, colFile2, fs);
    }
    
}
