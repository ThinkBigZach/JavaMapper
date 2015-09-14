import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class FileMapper {

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
    public static Path getManifestPaths(String pathToControl) throws IOException {
        fs = FileSystem.newInstance(new Configuration());
        mapping = new HashMap<String, ArrayList<String>>();

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
        writeOutManifestLocations();
        return null;
    }
    public static void main(String[] args) throws IOException {
        testing = args[0];
        entity = args[1];
        getValidPracticeIds();
        getValidEntityNames();
        getManifestPaths(args[0]);
    }



    public static void getValidPracticeIds() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToValidPractices))));
        String line = "";
        while((line = br.readLine()) != null){
            String validPractice = line.substring(0, line.indexOf("~"));
            validPracticeIDs.add(validPractice);
        }
    }



    public static void getValidEntityNames() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(pathToTableDefs))));
        String line = "";
        while((line = br.readLine()) != null){
            validEntityNames.add(line);
        }
    }


    public static boolean isValidEntry(String practiceID, String entityName, String schema){
        return isValidPractice(practiceID) && isValidEntity(entityName) && isValidSchema(schema);
    }

    public static boolean isValidPractice(String practiceID){
        if(validPracticeIDs.contains(practiceID)){
            return true;
        }
        return false;
    }

    public static boolean isValidEntity(String entityName){
        if(validEntityNames.contains(entityName)){
            return true;
        }
        return false;
    }

    public static boolean isValidSchema(String schema){
//            TODO VALIDATE SCHEMA
        return true;
    }

    public static String generateNewPath(Path path, String practiceID){
        String fixedPath = path.toString().substring(0, path.toString().indexOf("Manifest"));
        String testing2 = fixedPath.substring(0, fixedPath.indexOf("/data/") + 6);
        String testing3 = fixedPath.substring(fixedPath.indexOf("/athena/"));
        String newPath = testing2 + practiceID + testing3;
        return newPath;
    }

    public static void writeOutManifestLocations() throws IOException {
        for(Path p : manifestFiles){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = "";
            int lineCount = 0;
            while((line = br.readLine()) != null){
                if(lineCount > 3) {
                    //REPLACES THE DIVISION ID WITH THE PRACTICE ID FOR EACH LINE IN THE MANIFEST FILES
                    if(Integer.parseInt(line.split("\037")[1]) > 0) {
                        practiceID = line.substring(0, line.indexOf(".asv"));
                        fileName = line.substring(0, line.indexOf(".asv") + 4);
                        entity = line.substring(0, line.indexOf("_"));
                        while (practiceID.contains("_")) {
                            practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
                        }
                        String newPath = generateNewPath(p, practiceID);
                        if (mapping.containsKey(entity)) {
                            mapping.get(entity).add(newPath + fileName);
                        } else {
                            ArrayList<String> newList = new ArrayList<String>();
                            newList.add(newPath + fileName);
                            mapping.put(entity, newList);
                        }
                    }
                }
                lineCount++;
            }
            for(String s: mapping.keySet()){
                if(!fs.exists(new Path(outPath + s + ".txt"))){
                    fs.createNewFile(new Path(outPath + s + ".txt"));
                }
                FSDataOutputStream out = fs.append(new Path(outPath + s + ".txt"));
                for(String link : mapping.get(s)){
                    out.writeUTF(link + "\n");
                }
                out.close();
            }
        }
    }


    public static void readDateWildCard(Path pathToFiles, boolean wildCarded) throws IOException {
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

    public static void readDivisionalWildcard(String divisionPart, String datePart) throws IOException {
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
                    System.out.println("PATH" + temp + "\nDoes not exist!");

                }
            }
        }
    }
}