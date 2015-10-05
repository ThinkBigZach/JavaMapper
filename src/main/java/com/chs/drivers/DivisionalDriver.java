package com.chs.drivers;

import com.chs.utils.ChsUtils;
import com.chs.utils.PiiObfuscator;
import com.chs.utils.SchemaMatcher;
import com.chs.utils.SchemaRecord;
import com.chs.utils.TDConnector;
import com.google.common.base.Splitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.lf5.util.DateFormatManager;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DivisionalDriver implements Driver {

    //ascii replacement args
    private static final String UNIT_SEPARATOR = ChsUtils.UNIT_SEPARATOR;
    private static final String RECORD_SEPARATOR = ChsUtils.RECORD_SEPARATOR;
	
	//Constructor Args
	private String input_path;
	private String entity;
    private String inputParamEntity;
	private String out_path;
	private String practiceMap_path;
	private String entityMap_path;
    private String divisional_path;
	private String TD_Host;
	private String TD_User;
	private String TD_Password;
	private String TD_Database;
	
	//unsorted Args
    private String fileName;
    private FileSystem fs;
    private Set<Path> manifestFiles;
    private Set<Path> controlFiles;
    private ArrayList<String> errorArray;
    private ArrayList<String> validPracticeIDs;
    private ArrayList<String> validDivisionIDs;
    private ArrayList<String> validEntityNames;
    private HashMap<String, Set<String>> mapping;
    private Map<String, Integer> columnCounts;




public DivisionalDriver(String[] args) {
	input_path = args[0];
	entity = args[1];
    inputParamEntity = args[1];
	out_path = args[2];
	practiceMap_path = args[3]; 
	entityMap_path = args[4];
    divisional_path = args[5];
	TD_Host = args[6];
	TD_User = args[7];
	TD_Password = args[8];
	TD_Database = args[9];
	manifestFiles = new LinkedHashSet<Path>(6, (float) 1);
	controlFiles = new LinkedHashSet<Path>(200, (float) .91);
	errorArray = new ArrayList<String>();
	validPracticeIDs = new ArrayList<String>();
	validEntityNames = new ArrayList<String>();
    validDivisionIDs = new ArrayList<String>();
}



    private void getManifestPaths(String pathToControl) throws IOException {
        //MEANS A DIVISION WILDCARD
        if(pathToControl.contains("data/*")){
            String tempPath = pathToControl.substring(0, pathToControl.indexOf("/*"));
            readDivisionalWildcard(tempPath, pathToControl.substring(pathToControl.indexOf("/*") + 2));
        }
        //FOR THE DATE WILD CARD USE CASE
        else if(pathToControl.endsWith("*")){
            String temp = pathToControl;
            while(temp.contains("/")){
                temp = temp.substring(temp.indexOf("/") + 1);
            }
            String dateWildCard = temp.substring(0, temp.length() - 1);
            pathToControl = pathToControl.substring(0, pathToControl.indexOf(dateWildCard));
            readDateWildCard(new Path(pathToControl), dateWildCard, true);
        }
        else{
            readDateWildCard(new Path(pathToControl), null, false);
        }
        removeUnusedControlFiles();
//        System.out.println("NUM MANIFEST FILES TO PROCESS " + manifestFiles.size());

        writeOutFileLocations(manifestFiles, "Manifest");
//        System.out.println("NUM CONTROL FILES TO PROCESS " + controlFiles.size());
        writeOutFileLocations(controlFiles, "Control");

    }

    public void removeUnusedControlFiles(){
        ArrayList<Path> newControl = new ArrayList<Path>();
        for(Path p : controlFiles){
            String id = p.toString().substring(p.toString().indexOf("data/") + 5, p.toString().indexOf("/athena"));
            for(Path path : manifestFiles){
                if(path.toString().contains(id)){
                   newControl.add(p);
                    break;
                }
            }
        }
        controlFiles.clear();
        for(Path p : newControl){
            controlFiles.add(p);
        }
    }

    private void readAndLoadEntities(Set<String> paths, String entity) throws IOException {
        System.out.println("WRITING FILE FOR ENTITY " + entity);
        String entityOutpath = out_path + "/" + entity.toLowerCase() + "/";
        String outFileNameMili = appendTimeAndExtension(entityOutpath + entity);
        String errOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/" + entity.toLowerCase() + "/";
        String errFileNameMili = appendTimeAndExtension(errOutpath + entity);

        if(!fs.exists(new Path(entityOutpath))){
            fs.mkdirs(new Path(entityOutpath));
        }
        if(!fs.exists(new Path(outFileNameMili))){
            fs.createNewFile(new Path(outFileNameMili));
        }
        FSDataOutputStream out = fs.append(new Path(outFileNameMili));
        FSDataOutputStream err = null;
        for(String path : paths) {
            String jobId = getJobIdFromPaths(path);
            String myFileName = path.substring(path.lastIndexOf("/") + 1);
            Scanner fileScanner = new Scanner(fs.open(new Path(path)));
            fileScanner.useDelimiter(RECORD_SEPARATOR);
            String line = "";
            int lineCount = 0;
            String headerInfo = null;
            Map<String, List<SchemaRecord>> schemas = SchemaMatcher.goldenEntitySchemaMap;
            boolean needsProcess = PiiObfuscator.hasRemoveComment(schemas.get(entity.toLowerCase()));
            Pattern validPattern = null;
            while(fileScanner.hasNextLine()) {

                line = fileScanner.next();
                if(lineCount == 0){
                    headerInfo = line;

                    if (!validateColumnCounts(entity, new Path(path).toString(), fs))
                    {
                    	errorArray.add(path);
                    	break;
                    }
                }
                if(lineCount == 1){
                     validPattern = Pattern.compile(ChsUtils.getPatternMatch(line.replaceAll(RECORD_SEPARATOR, "").trim()));
                }
                if (lineCount > 3 && line.trim().length() > 0) {
                	String cleanLine = ChsUtils.replaceCRandLF(line);
                	if (needsProcess)
                	{
                		cleanLine = PiiObfuscator.piiProcess(cleanLine.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR), schemas.get(entity.toLowerCase()), UNIT_SEPARATOR);
                	}
                    Matcher m = validPattern.matcher(cleanLine);
                	boolean isGoodLine = m.matches();
                	int cl_int = Splitter.on(UNIT_SEPARATOR).splitToList(cleanLine).size();
                	int he_int = Splitter.on(UNIT_SEPARATOR).splitToList(headerInfo).size();
                	if(cl_int == he_int + 3 && isGoodLine) {
                		String lineclone = cleanLine;
                		if (needsDynamicSchemaReorder(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), headerInfo.split(UNIT_SEPARATOR)))
                		{
                			lineclone = reorderAlongSchema(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), cleanLine.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR));                			
                		}
                		lineclone = lineclone + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + myFileName;
                		System.out.println(String.format("BEFORE: \n\t%s \nAFTER: \n\t%s", cleanLine, lineclone));
                		out.write((lineclone + "\n").getBytes());
                	}
                	else {
                        if(!fs.exists(new Path(errOutpath))){
                            fs.mkdirs(new Path(errOutpath));
                        }
                        if(!fs.exists(new Path(errFileNameMili))){
                            fs.createNewFile(new Path(errFileNameMili));
                            err = fs.append(new Path(errFileNameMili));
                        }
                        else if(err == null){
                            err = fs.append(new Path(errFileNameMili));
                        }
                		err.write((cleanLine + "\n").getBytes());
                	}
                }
                lineCount++;
            }
        }
        if(err != null){
            err.close();
        }
        out.close();
    }

    private boolean needsDynamicSchemaReorder(Map<String,Integer> goldSchema, String[] headerinfo)
    {
    	for(int i = 0; i < headerinfo.length; i++)
    	{
    		String cleanhead = headerinfo[i].replace(" ", "_").toLowerCase();
    		if (goldSchema.get(cleanhead) != i)
    		{
    			return true;
    		}
    	}
    	return false;
    }
    
    private String reorderAlongSchema(Map<String, Integer> goldSchema, String[] schemaColumns, String[] headerinfo)
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

    private String getJobIdFromPaths(String path) {
        String temp = path.substring(0, path.lastIndexOf('/'));
        temp = temp+"/CONTROL.TXT";

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(temp))));
            String line = br.readLine();
            if (!line.isEmpty()) {
                return Splitter.on('~').splitToList(line).get(3);
            }
        } catch (IOException e) {
//            e.printStackTrace();
//        	System.out.println("returnCode=FAILURE");
        }
        return "";

    }

    private boolean validateColumnCounts(String entity, String colFile2, FileSystem fs) throws FileNotFoundException{
    	return SchemaMatcher.matchSchemas(entity, colFile2, fs);
    }

    private  void getValidPracticeIds() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(practiceMap_path))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice.toUpperCase());
        }
    }

    private void getValidEntityNames() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(entityMap_path))));
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


    private void getValidDivisionIds() throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(divisional_path))));
        String line = "";
        while((line = br.readLine()) != null){
            validDivisionIDs.add(Splitter.on('~').splitToList(line).get(0));
        }
    }

    private  boolean isValidEntry(String practiceID, String entityName){
        return isValidPractice(practiceID) && isValidEntity(entityName);
    }

    private boolean isValidDivision(String divisionID){
        return validDivisionIDs.contains(divisionID);
    }

    private  boolean isValidPractice(String practiceID){
        boolean isValid = validPracticeIDs.contains(practiceID.toUpperCase());
        return isValid;
    }

    private  boolean isValidEntity(String entityName){
    	boolean isValid = validEntityNames.contains(entityName.toUpperCase());
    	return isValid;
    }


    private String generateNewPath(Path path, String practiceID){
        String fixedPath = path.toString().substring(0, path.toString().indexOf("Manifest"));
        String testing2 = fixedPath.substring(0, fixedPath.indexOf("/data/") + 6);
        String testing3 = fixedPath.substring(fixedPath.indexOf("/athena/"));
        String newPath = testing2 + practiceID + testing3;
        return newPath;
    }

    private void writeOutFileLocations(Set<Path> files, String type) throws IOException {
        String manconOutpath = out_path +"/" +  type.toLowerCase() + "/"  + type;
        String outFileNameMili = appendTimeAndExtension(manconOutpath);
        if (!fs.exists(new Path(outFileNameMili))) {
            fs.createNewFile(new Path(outFileNameMili));
        }
        String myFile = "";
    	for(Path p : files){
    		String jobId = getJobIdFromPaths(p.toString());
            Scanner fileScanner = new Scanner(fs.open(p));
            fileScanner.useDelimiter(RECORD_SEPARATOR);
            String line = "";
            int current_line = 0;

            while(fileScanner.hasNextLine()) {
                line = fileScanner.next();

                if (current_line > 3 && type.equalsIgnoreCase("MANIFEST")) {
                    processLine(p, line);
                    myFile += ChsUtils.replaceCRandLF(line) + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + p.getName() + "\n";
                }
                else if(type.equalsIgnoreCase("CONTROL")){
                	String fixedLine = ChsUtils.replaceCRandLF(line);
                	fixedLine = fixedLine.replaceAll("~", UNIT_SEPARATOR);
                    myFile += fixedLine + "\n";
                }
                current_line++;
            }
        }
            FSDataOutputStream out = fs.append(new Path(outFileNameMili));
            out.write(myFile.getBytes());
            out.close();
    }
    
    private void processLine(Path p, String line) {
    	String practiceID;

        line = line.replaceFirst("^\\s+", "");
        line =  ChsUtils.replaceCRandLF(line);
        List<String> splitValue = Splitter.on(UNIT_SEPARATOR).splitToList(line);
        if (splitValue.size() > 1) {

            if (Integer.parseInt(splitValue.get(1)) > 0) {
                practiceID = line.substring(0, line.indexOf(".asv"));
                fileName = line.substring(0, line.indexOf(".asv") + 4);

                entity = line.substring(0, line.indexOf("_"));
                while (practiceID.contains("_")) {
                    practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
                }
                String newPath = generateNewPath(p, practiceID);
                if (isValidEntry(practiceID, entity)) {
                    addToMapping(newPath);

                    //Creates a path for CONTROL.TXT in each Practice ID Folder
                    Path controlPath = new Path(newPath + "CONTROL.TXT");
                    if(!controlFiles.contains(controlPath)){
                        controlFiles.add(controlPath);
                    }
                }
            }
        }
    }

    private  void addToMapping(String newPath){
        if (mapping.containsKey(entity.toUpperCase())) {
            mapping.get(entity.toUpperCase()).add(newPath + fileName);
        } else {
            Set<String> newList = new LinkedHashSet<String>();
            newList.add(newPath + fileName);
            mapping.put(entity.toUpperCase(), newList);
        }
    }
    
    private  void readDateWildCard(Path pathToFiles, String dateWildCard, boolean wildCarded) throws IOException {
        String divisionId = pathToFiles.toString().substring( pathToFiles.toString().indexOf("/data/") + 6);
        divisionId = divisionId.substring(0, divisionId.indexOf("/"));
        if(fs.exists(pathToFiles) && isValidDivision(divisionId)) {
            FileStatus[] fileStatuses = fs.listStatus(pathToFiles);
            for (FileStatus status : fileStatuses) {
                if (status.isDirectory()) {
                    if (wildCarded) {
                        if (status.getPath().getName().startsWith(dateWildCard)) {
                            readDateWildCard(status.getPath(), dateWildCard, false);
                        }
                    } else {
                        readDateWildCard(status.getPath(), dateWildCard, false);
                    }
                } else {
                    if (status.getPath().toString().toUpperCase().contains("MANIFEST")) {
                        manifestFiles.add(status.getPath());
                    } else if (status.getPath().toString().toUpperCase().contains("CONTROL")) {
                        controlFiles.add(status.getPath());
                    }
                }
            }
        }
    }




    private void readDivisionalWildcard(String divisionPart, String datePart) throws IOException {
        //FIRST READ IN ALL DIVISION FOLDERS
        FileStatus[] fileStatuses = fs.listStatus(new Path(divisionPart));
//        System.out.println(divisionPart);

        for(FileStatus status : fileStatuses){

            if(status.isDirectory() && isValidDivision(status.getPath().getName())) {

                //Reads the directory and appends the date part
                String temp = status.getPath().toString() + datePart;

                if (temp.endsWith("*")) {

                    //DATE IS WILDCARDED TOO
                    String dateWildCard = temp.substring(temp.lastIndexOf("/") + 1);
                    dateWildCard = dateWildCard.substring(0, dateWildCard.length() - 1);

                    readDateWildCard(new Path(temp.substring(0, temp.indexOf(dateWildCard))), dateWildCard, true);
                } else {

                    //DATE ISN'T WILDCARDED READ NORMALLY
                    try {
                        FileStatus[] dateFiles = fs.listStatus(new Path(temp));
                        for (FileStatus dateStatus : dateFiles) {
                            if (dateStatus.getPath().toString().toUpperCase().contains("MANIFEST")) {
                                manifestFiles.add(dateStatus.getPath());
                            } else if (dateStatus.getPath().toString().toUpperCase().contains("CONTROL")) {
                                controlFiles.add(dateStatus.getPath());
                            }
                        }
                    } catch (Exception e) {
//                    	System.out.println("PATH " + temp + "Does not exist!");
//                    	System.out.println("returnCode=FAILURE");
                    }
                }
            }
            }
        }

    /*
     * (non-Javadoc)
     * @see com.chs.drivers.Driver#start()
     * core launch method. All logic for divisional load needs to be in or called from this method
     */
    public void start()  {
        System.out.println("CURRENT TIME IN MILLIS IS:" + System.currentTimeMillis());
        long startTime = System.currentTimeMillis();
        TDConnector.init(TD_Host, TD_User, TD_Password, TD_Database);
        TDConnector.getConnection();
        try {
            fs = FileSystem.newInstance(new Configuration());
            mapping = new HashMap<String, Set<String>>();
            this.getValidPracticeIds();
            this.getValidEntityNames();
            this.getValidDivisionIds();
            System.out.println("GOT ENTITIES AND PRACTICES");
            this.getManifestPaths(input_path);
            System.out.println("GOT MANIFEST PATHS");
            try {
                long startWrite = System.currentTimeMillis();
                //TODO: This can be threaded to somehow work with the readAndLoadEntities
                for (String s : mapping.keySet()) {
                    if(s.equalsIgnoreCase(inputParamEntity) || inputParamEntity.equalsIgnoreCase("")) {
                        this.readAndLoadEntities(mapping.get(s), s);
                    }
                }
                long endWrite = System.currentTimeMillis();
//                System.out.println(((endWrite - startWrite)/1000) + " seconds to execute writing the files");

            } catch (Exception e) {
                e.printStackTrace();
            	System.out.println("returnCode=FAILURE");
            }
            long endTime = System.currentTimeMillis();
            System.out.println(((endTime - startTime)/1000) + " seconds to execute entire request");
            writeErrorFiles();
        }
        catch(IOException e){
            e.printStackTrace();
        	System.out.println("returnCode=FAILURE");
        }
    }

    private void writeErrorFiles() throws IllegalArgumentException, IOException {
    	String tempOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/";
        if (!fs.exists(new Path(tempOutpath))) {
            fs.mkdirs(new Path(tempOutpath));
        }
        if(errorArray.size() > 0) {
	        Date tempDate = new Date(System.currentTimeMillis());
	        String yyyymmddhhmmss = new DateFormatManager().format(tempDate, "yyyy-MM-dd-hh-mm-ss");
	        String errorOutpath = tempOutpath + "error-" + yyyymmddhhmmss + ".txt";
	        if (!fs.exists(new Path(errorOutpath))) {
	            fs.createNewFile(new Path(errorOutpath));
	        }
	        FSDataOutputStream out = fs.append(new Path(errorOutpath));
			for (String line : errorArray) {
				out.write((line).getBytes());
			}       
			out.close();
        }
	}

	private String appendTimeAndExtension(String s) {

        String time = "."+System.currentTimeMillis();
        return s += time + ".txt";

    }

}