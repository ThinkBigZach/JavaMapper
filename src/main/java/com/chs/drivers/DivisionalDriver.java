package com.chs.drivers;

import com.chs.utils.HiveConnector;
import com.chs.utils.TDConnector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DivisionalDriver implements Driver {

    //ascii replacement args
    private final String CR = "\012"; //carriage return
    private final String LF = "\015"; //line feed
    private final String UNIT_SEPERATOR = "\031";
	
	//Constructor Args
	private String input_path;
	private String entity;
    private String entity1;
	private String out_path;
	private String practiceMap_path;
	private String entityMap_path;
	private String TD_Host;
	private String TD_User;
	private String TD_Password;
	private String TD_Database;
	
	//unsorted Args
    private String fileName;
    private FileSystem fs;
    private boolean isWildcard = false;
    private String dateWildCard;
    private ArrayList<Path> manifestFiles;
    private ArrayList<Path> controlFiles;
    private ArrayList<String> errorArray;
    private ArrayList<String> validPracticeIDs;
    private ArrayList<String> validEntityNames;
    private HashMap<String, ArrayList<String>> mapping;
    private Map<String, Integer> columnCounts;


public DivisionalDriver(String[] args) {
	input_path = args[0];
	entity = args[1];
    entity1 = args[1];
	out_path = args[2];
	practiceMap_path = args[3]; 
	entityMap_path = args[4]; 
	TD_Host = args[5];
	TD_User = args[6]; 
	TD_Password = args[7]; 
	TD_Database = args[8];
	manifestFiles = new ArrayList<Path>();
	controlFiles = new ArrayList<Path>();
	errorArray = new ArrayList<String>();
	validPracticeIDs = new ArrayList<String>();
	validEntityNames = new ArrayList<String>();
}
    
    private void getManifestPaths(String pathToControl) throws IOException {
        //MEANS A DIVISION WILDCARD
        if(pathToControl.contains("data/*")){
            String tempPath = pathToControl.substring(0, pathToControl.indexOf("/*"));
            readDivisionalWildcard(tempPath, pathToControl.substring(pathToControl.indexOf("/*") + 2));
        }
        //FOR THE DATE WILD CARD USE CASE
        else if(pathToControl.endsWith("*")){
            isWildcard = true;
            String temp = pathToControl;
            while(temp.contains("/")){
                temp = temp.substring(temp.indexOf("/") + 1);
            }
            dateWildCard = temp.substring(0, temp.length() - 1);
            pathToControl = pathToControl.substring(0, pathToControl.indexOf(dateWildCard));
            readDateWildCard(new Path(pathToControl), true);
        }
        else{
            readDateWildCard(new Path(pathToControl), false);
        }
//           TODO FIX LOGIC FOR EXCESS CONTROL FILES
        removeUnusedControlFiles();
        System.out.println("NUM MANIFEST FILES TO PROCESS " + manifestFiles.size());
        System.out.println("NUM CONTROL FILES TO PROCESS " + controlFiles.size());

        writeOutFileLocations(manifestFiles, "Manifest");
        writeOutFileLocations(controlFiles, "Control");
        return;
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

    private void readAndLoadEntities(ArrayList<String> paths, String entity) throws IOException {
        System.out.println("WRITING FILE FOR ENTITY " + entity);
        String entityOutpath = out_path + entity + "/";
        if(!fs.exists(new Path(entityOutpath + entity + ".txt"))){
            fs.createNewFile(new Path(entityOutpath + entity + ".txt"));
        }
        FSDataOutputStream out = fs.append(new Path(entityOutpath + entity + ".txt")); 
        //TODO:  This could be paralellized or a thread for each path, see the caller above.
        for(String path : paths){
            String jobId = getJobIdFromPaths(path);
            System.out.println("JOB ID IS:  " +jobId);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line = "";
            int lineCount = 0;
            while((line = br.readLine()) != null){
                if(lineCount == 1){
//                    TODO: THROW OUT FILE IF COLUMN COUNTS DONT MATCH WHEN GARY GETS BACK TO US
                    validateColumnCounts(line, entity);

                }
                if (lineCount > 3) {
                    String fixedLine = replaceCRandLF(line);
                    //add row entry (default to 0 for now), jobId, fileName;
                    fixedLine = fixedLine + UNIT_SEPERATOR + "0" + UNIT_SEPERATOR + jobId + UNIT_SEPERATOR + fileName;
                    out.write((fixedLine + "\n").getBytes());
                }
                lineCount++;
            }
        }
        out.close();
    }

    private String getJobIdFromPaths(String path) {
        String temp = path.substring(0,path.lastIndexOf('/'));
        temp = temp+"/CONTROL.TXT";

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(temp))));
            String line = br.readLine();
            if (!line.isEmpty()) {
                return line.split("~")[3];
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";

    }

//TODO SWITCH FROM return true; to return line.split("\037").length == columnCounts.get(entity); WHEN GARY GETS BACK TO US
    private boolean validateColumnCounts(String line, String entity){
        return true;
//        return line.split("\037").length == columnCounts.get(entity);
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
            validEntityNames.add(line.toUpperCase());
        }
    }


    private  boolean isValidEntry(String practiceID, String entityName, String schema){
        return isValidPractice(practiceID) && isValidEntity(entityName) && validateColumnCounts(schema, entityName);
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

    private void writeOutFileLocations(ArrayList<Path> files, String type) throws IOException {
        for(Path p : files){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = "";
            int lineCount = 0;
            String myFile = "";
            while((line = br.readLine()) != null) {
                if (lineCount > 3) {
                    if (type.equalsIgnoreCase("MANIFEST")) {
                        processLine(p, line);
                    }
                }
                myFile += replaceCRandLF(line) + "\n";
                lineCount++;
            }
            if (!fs.exists(new Path(out_path + type + ".txt"))) {
                fs.createNewFile(new Path(out_path + type + ".txt"));
            }
            FSDataOutputStream out = fs.append(new Path(out_path + type + ".txt"));
            out.write(myFile.getBytes());
            out.close();
        }
        try {
            HiveConnector.loadTable(type, out_path + type + ".txt");
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    private void processLine(Path p, String line){
    	String practiceID;
        if(Integer.parseInt(line.split("\037")[1]) > 0) {
            practiceID = line.substring(0, line.indexOf(".asv"));
            fileName = line.substring(0, line.indexOf(".asv") + 4);
            entity = line.substring(0, line.indexOf("_"));
            while (practiceID.contains("_")) {
                practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
            }
            String newPath = generateNewPath(p, practiceID);
            if(isValidEntry(practiceID,entity, null)) {
                addToMapping(newPath);
            }
            else{
                errorArray.add(newPath);
            }
        }
    }

    private  void addToMapping(String newPath){
        if (mapping.containsKey(entity.toUpperCase())) {
            mapping.get(entity.toUpperCase()).add(newPath + fileName);
        } else {
            ArrayList<String> newList = new ArrayList<String>();
            newList.add(newPath + fileName);
            mapping.put(entity.toUpperCase(), newList);
        }
    }


    private  String replaceCRandLF(String line){
        line = line.replaceAll(CR, "");
        line = line.replaceAll(LF, "");
        return line;
    }
    
    private  void readDateWildCard(Path pathToFiles, boolean wildCarded) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(pathToFiles);
        for(FileStatus status : fileStatuses){
            if(status.isDirectory()){
                if(wildCarded) {
                    if (status.getPath().getName().startsWith(dateWildCard)) {
                        readDateWildCard(status.getPath(), false);
                    }
                }
                else{
                    readDateWildCard(status.getPath(), false);
                }
            }
            else{
                if (status.getPath().toString().toUpperCase().contains("MANIFEST")){
                    manifestFiles.add(status.getPath());
                }
                else if(status.getPath().toString().toUpperCase().contains("CONTROL")){
                    controlFiles.add(status.getPath());
                }
            }
        }
    }

    private void readDivisionalWildcard(String divisionPart, String datePart) throws IOException {
        //FIRST READ IN ALL DIVISION FOLDERS
        FileStatus[] fileStatuses = fs.listStatus(new Path(divisionPart));
        for(FileStatus status : fileStatuses){
            if(status.isDirectory()){

                //Reads the directory and appends the date part
                String temp =  status.getPath().toString() + datePart;
                try {
                    FileStatus[] dateFiles = fs.listStatus(new Path(temp));
                    for (FileStatus dateStatus : dateFiles) {
                        if (dateStatus.getPath().toString().toUpperCase().contains("MANIFEST")) {
                            manifestFiles.add(dateStatus.getPath());
                        }
                        else if(dateStatus.getPath().toString().toUpperCase().contains("CONTROL")){
                            controlFiles.add(dateStatus.getPath());
                        }
                    }
                }
                catch(Exception e){
//                    System.out.println("PATH " + temp + "Does not exist!");
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
            try {
                columnCounts = TDConnector.getColumnCounts();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            mapping = new HashMap<String, ArrayList<String>>();
            this.getValidPracticeIds();
            this.getValidEntityNames();
            System.out.println("GOT ENTITIES AND PRACTICES");
            this.getManifestPaths(input_path);
            System.out.println("GOT MANIFEST PATHS");
            try {
                long startWrite = System.currentTimeMillis();
                //TODO: This can be threaded to somehow work with the readAndLoadEntities
                for (String s : mapping.keySet()) {
                    if(s.equalsIgnoreCase(entity1) || entity1.equalsIgnoreCase("")) {
                        this.readAndLoadEntities(mapping.get(s), s);
                    }
                }
                long endWrite = System.currentTimeMillis();
                System.out.println(((endWrite - startWrite)/1000) + " seconds to execute writing the files");

            } catch (Exception e) {
                e.printStackTrace();
            }
            long startWrite = System.currentTimeMillis();
            for (String s : mapping.keySet()) {
                if(s.equalsIgnoreCase(entity) || entity.equalsIgnoreCase("")) {
                    try {
                    	String entityOutpath = out_path + s + "/";
                        HiveConnector.createEntityTables(s, entityOutpath);//out_path);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            long endWrite = System.currentTimeMillis();
            System.out.println(((endWrite - startWrite) / 1000) + " seconds to execute loading the files");
            long endTime = System.currentTimeMillis();
            System.out.println(((endTime - startTime)/1000) + " seconds to execute entire request");
        }
        catch(IOException e){
            System.out.println(e.getMessage());

        }
    }
}