import drivers.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.HiveConnector;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class FileMapper implements Driver {

    static String testing ="hdfs://quickstart.cloudera:8020/financialDataFeed/data/8764/athena/finished/2011-07-01-2011-07-31/";
    static String practiceID;
    static String outPath = "/user/rscott22/mapping/";
    static String entity;
    static String fileName;
    static FileSystem fs;
    static ArrayList<Path> manifestFiles = new ArrayList<Path>();
    static ArrayList<Path> controlFiles = new ArrayList<Path>();
    static HashMap<String, ArrayList<String>> mapping;
    static boolean isWildcard = false;
    static String pathToTableDefs = "/enterprise/mappings/athena/athena_table_defs.csv";
    static String pathToValidPractices = "/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv";
    static String dateWildCard = "";
    static ArrayList<String> errorArray = new ArrayList<String>();
    static ArrayList<String> validPracticeIDs = new ArrayList<String>();
    static ArrayList<String> validEntityNames = new ArrayList<String>();
    private  Path getManifestPaths(String pathToControl) throws IOException {
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
        writeOutFileLocations(manifestFiles, "Manifest");
        writeOutFileLocations(controlFiles, "Control");
        return null;
    }
    public  void main(String[] args) throws IOException {
        testing = args[0];
        entity = args[1];
        for(String s : args){
            System.out.println("ARGS ARE " + s);
        }
        fs = FileSystem.newInstance(new Configuration());

        System.out.println("INITIALIZED FS");
        mapping = new HashMap<String, ArrayList<String>>();
        getValidPracticeIds();
        getValidEntityNames();
        System.out.println("GOT ENTITIES AND PRACTICES");
        getManifestPaths(args[0]);
        System.out.println("GOT MANIFEST PATHS");
        loadEntities("");
    }



    private void readAndLoadEntities(ArrayList<String> paths) throws IOException {
        for(String path : paths){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line = "";
            while((line = br.readLine()) != null){
                String fixedLine = replaceCRandLF(line);




            }
        }


    }


    private  void getValidPracticeIds() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToValidPractices))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice);
        }
    }

    private void getValidEntityNames() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToTableDefs))));
        String line = "";
        while((line = br.readLine()) != null){
            validEntityNames.add(line);
        }
    }


    private  boolean isValidEntry(String practiceID, String entityName, String schema){
        return isValidPractice(practiceID) && isValidEntity(entityName) && isValidSchema(schema);
    }

    private  boolean isValidPractice(String practiceID){
        if(validPracticeIDs.contains(practiceID)){
            return true;
        }
        return false;
    }

    private  boolean isValidEntity(String entityName){
        if(validEntityNames.contains(entityName)){
            return true;
        }
        return false;
    }

    private  boolean isValidSchema(String schema){
//            TODO VALIDATE SCHEMA
        return true;
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
                if (!fs.exists(new Path(outPath + type + ".txt"))) {
                    fs.createNewFile(new Path(outPath + type + ".txt"));
                }
                myFile += replaceCRandLF(line) + "\n";
                lineCount++;
            }
            FSDataOutputStream out = fs.append(new Path(outPath + type + ".txt"));
            out.write(myFile.getBytes());
            out.close();
        }
        try {
            HiveConnector.loadTable(type, outPath + type + ".txt");
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }


    private void loadEntities(String entity) throws IOException {
        for (String s : mapping.keySet()) {
            if(entity.equals("") || s.equalsIgnoreCase(entity)) {
                if (!fs.exists(new Path(outPath + s + ".txt"))) {
                    fs.createNewFile(new Path(outPath + s + ".txt"));
                }
                FSDataOutputStream out = fs.append(new Path(outPath + s + ".txt"));
                for (String link : mapping.get(s)) {
                    try {
                        HiveConnector.loadTable("Entity", s, link);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                out.close();
            }
        }
    }

    private void processLine(Path p, String line){
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
        if (mapping.containsKey(entity)) {
            mapping.get(entity).add(newPath + fileName);
        } else {
            ArrayList<String> newList = new ArrayList<String>();
            newList.add(newPath + fileName);
            mapping.put(entity, newList);
        }
    }

    private  String replaceCRandLF(String line){
        line = line.replaceAll("\012", "");
        line = line.replaceAll("\015", "");
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
                if(status.getPath().toString().contains("Manifest")){
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
                        if(dateStatus.getPath().toString().contains("Manifest")) {
                            manifestFiles.add(dateStatus.getPath());
                        }
                        else if(status.getPath().toString().toUpperCase().contains("CONTROL")){
                            controlFiles.add(dateStatus.getPath());
                        }
                    }
                }
                catch(Exception e){
                    System.out.println("PATH " + temp + "Does not exist!");
                }
            }
        }
    }

    //@Override
    public void start(String[] args)  {
        testing = args[0];
        entity = args[1];
        try {
            fs = FileSystem.newInstance(new Configuration());
            mapping = new HashMap<String, ArrayList<String>>();
            getValidPracticeIds();
            getValidEntityNames();
            getManifestPaths(args[0]);
        }
        catch(Exception e){

        }
    }
}