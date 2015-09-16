package com.chs;

import com.chs.drivers.Driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.chs.utils.HiveConnector;
import com.chs.utils.TDConnector;

import java.io.BufferedReader;
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
    static TDConnector teradata;


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

        for(Path p : manifestFiles){
            System.out.println("MANIFEST FILES ARE: " + p.toString());
        }
        for(Path p : controlFiles){
            System.out.println("CONTROL FILES ARE: " + p.toString());
        }
        writeOutFileLocations(manifestFiles, "Manifest");
        writeOutFileLocations(controlFiles, "Control");
        return null;
    }
// args[0] -- INPUT PATH LIKE THIS - /user/financialDataFeed/data/*/finished/yyyy-mm-dd[-yyyy-mm-dd]
    //args[1] -- entity like this - allergy or "" "" will give you all entities
    //args[2] -- outpath like this -- /user/athena/financialdatafeed/extracted/finished
    //args[3] -- valid practice map location like this --/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv
    //args[4] -- valid entity map location like this -- /enterprise/mappings/athena/athena_table_defs.csv
    //TD_HOST - args[5] -dev.teradata.chs.net
    //TD_USER - args[6] - dbc
    //TD_PSWD - args[7] - dbc
    //TD_DATABASE - args[8] -- EDW_ATHENA_STAGE

    /**
     *hadoop jar input.jar /user/financialDataFeed/data/<star>/athena/finished/2015-09-13 allergy /user/rscott22/mapping/ /enterprise/mappings/athena/chs-practice-id-mapping-athena.csv /enterprise/mappings/athena/athena_table_defs.csv dev.teradata.chs.net dbc dbc EDW_ATHENA_STAGE
     *
     */
    public static void main(String[] args) throws IOException {
        for(String s : args){
            System.out.println("ARGS ARE " + s);
        }
        testing = args[0];
        entity = args[1];
        outPath = args[2];
        pathToValidPractices = args[3];
        pathToTableDefs = args[4];
//        teradata = new TDConnector(args[5], args[6], args[7], args[8]);
        fs = FileSystem.newInstance(new Configuration());
//        teradata.Connect();
        System.out.println("INITIALIZED FS");
        mapping = new HashMap<String, ArrayList<String>>();
        FileMapper fileMapper = new FileMapper();
        fileMapper.getValidPracticeIds();
        fileMapper.getValidEntityNames();
        System.out.println("GOT ENTITIES AND PRACTICES");
        fileMapper.getManifestPaths(args[0]);
        System.out.println("GOT MANIFEST PATHS");

        try {
            for(String s : mapping.keySet()){
                fileMapper.readAndLoadEntities(mapping.get(s), s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(String s :mapping.keySet()){
            try {
                HiveConnector.createEntityTables(s, outPath);
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

    }



    private void readAndLoadEntities(ArrayList<String> paths, String entity) throws IOException {
        System.out.println("WRITING FILE FOR ENTITY " + entity);
        if(!fs.exists(new Path(outPath + entity + ".txt"))){
            fs.createNewFile(new Path(outPath + entity + ".txt"));
        }
        FSDataOutputStream out = fs.append(new Path(outPath + entity + ".txt"));
        //TODO:  This could be paralellized or a thread for each path
        for(String path : paths){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String line = "";
            int lineCount = 0;
            while((line = br.readLine()) != null){
                if (lineCount > 3) {
                    String fixedLine = replaceCRandLF(line);
                    out.write((line + "\n").getBytes());
                }
                lineCount++;
            }
        }
        out.close();
    }


    private  void getValidPracticeIds() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToValidPractices))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice.toUpperCase());
        }
    }

    private void getValidEntityNames() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToTableDefs))));
        String line = "";
        while((line = br.readLine()) != null){
            validEntityNames.add(line.toUpperCase());
        }
    }


    private  boolean isValidEntry(String practiceID, String entityName, String schema){
        return isValidPractice(practiceID) && isValidEntity(entityName) && isValidSchema(schema);
    }

    private  boolean isValidPractice(String practiceID){
        if(validPracticeIDs.contains(practiceID.toUpperCase())){
            return true;
        }
        return false;
    }

    private  boolean isValidEntity(String entityName){
        if(validEntityNames.contains(entityName.toUpperCase())){
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
                myFile += replaceCRandLF(line) + "\n";
                lineCount++;
            }
            if (!fs.exists(new Path(outPath + type + ".txt"))) {
                fs.createNewFile(new Path(outPath + type + ".txt"));
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
        System.out.println("LOADING ENTITIES NOW");
        System.out.println("MAPPING SIZE" + mapping.keySet().size());
        System.out.println("MAPPING SIZE" + mapping.toString());

        for (String s : mapping.keySet()) {
            System.out.println("LOADING FROM FILE " + s);
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
//
//        ArrayList<String> paths = new ArrayList<String>();
//        paths.add(newPath);
//        try {
//            readAndLoadEntities(paths, entity);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    //TODO: create constants for these characters.
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