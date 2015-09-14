import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class FileMapper {

    static String testing ="hdfs://quickstart.cloudera:8020/financialDataFeed/data/8764/athena/finished/2011-07-01-2011-07-31/";
    static String practiceID;
    static String outPath = "/user/rscott22/mapping/";
//    static String outPath = "/user/athena/financialdatafeed/extracted/finished";
    static String entity;
    static String fileName;
    static FileSystem fs;
    static ArrayList<Path> manifestFiles = new ArrayList<Path>();
    static ArrayList<Path> controlFiles = new ArrayList<Path>();
    static HashMap<String, ArrayList<String>> mapping;
    static boolean isWildcard = false;
    static String dateWildCard = "";
    public static Path getControlPath(String pathToControl) throws IOException {
        fs = FileSystem.newInstance(new Configuration());
        mapping = new HashMap<String, ArrayList<String>>();

        if(pathToControl.contains("data/*")){
//        TODO IMPLEMENT THE DIVISION WILDCARD USE CASE
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
            readFilesFromPath(new Path(pathToControl), true);
        }
        for(Path p : manifestFiles){
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line = "";
            int lineCount = 0;
            while((line = br.readLine()) != null){
                for(String s : line.split("\037")){
                    System.out.println(s);
                }
                if(lineCount > 3) {
                    //REPLACES THE DIVISION ID WITH THE PRACTICE ID FOR EACH LINE IN THE MANIFEST FILES

                    practiceID = line.substring(0, line.indexOf(".asv"));
                    fileName = line.substring(0, line.indexOf(".asv") +4);
                    entity = line.substring(0, line.indexOf("_"));
                    while(practiceID.contains("_")) {
                        practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
                    }
                    String fixedPath = p.toString().substring(0, p.toString().indexOf("Manifest"));
                    String testing2 = fixedPath.substring(0, fixedPath.indexOf("/data/") + 6);
                    String testing3 = fixedPath.substring(fixedPath.indexOf("/athena/"));
                    String newPath = testing2 + practiceID + testing3;
                    if(mapping.containsKey(entity)){
                            mapping.get(entity).add(newPath + fileName);
                    }
                    else{
                            ArrayList<String> newList = new ArrayList<String>();
                            newList.add(newPath + fileName);
                            mapping.put(entity, newList);
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
        return null;
    }


    public static void main(String[] args) throws IOException {
        testing = args[0];
        entity = args[1];
        getControlPath(args[0]);
    }




    public static void readFilesFromPath(Path pathToFiles, boolean wildCarded) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(pathToFiles);
        for(FileStatus status : fileStatuses){
            if(status.isDirectory()){
                if(wildCarded) {
                    if (status.getPath().getName().startsWith(dateWildCard)) {
                        readFilesFromPath(status.getPath(), false);
                    }
                }
                else{
                    readFilesFromPath(status.getPath(), false);
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
}