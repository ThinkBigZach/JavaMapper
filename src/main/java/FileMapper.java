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
//    static String outPath = "/user/rscott22/mapping/";
    static String outPath = "/user/athena/financialdatafeed/extracted/finished";
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
//        FileStatus[] fileStatuses = fs.listStatus(new Path(pathToControl));
        if(pathToControl.endsWith("*")){
            isWildcard = true;
            String temp = pathToControl;

            while(temp.contains("/")){
                temp = temp.substring(temp.indexOf("/") + 1);
            }
            dateWildCard = temp;
            pathToControl = pathToControl.substring(0, pathToControl.indexOf(dateWildCard));
        }
        readFilesFromPath(new Path(pathToControl));
//        for(FileStatus status : fileStatuses){
//            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
//            String line = "";
//            System.out.println(status.getPath().toString());
//            int lineCount = 0;
//            while((line = br.readLine()) != null){
//                if(status.getPath().toString().contains("Manifest")){
//                    if(lineCount > 3) {
//                        practiceID = line.substring(0, line.indexOf(".asv"));
//                        fileName = line.substring(0, line.indexOf(".asv") +4);
//                        entity = line.substring(0, line.indexOf("_"));
//                        while(practiceID.contains("_")) {
//                            practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
//                        }
//                        String testing2 = testing.substring(0, testing.indexOf("/data/") + 6);
//                        String testing3 = testing.substring(testing.indexOf("/athena/"));
//                        String newPath = testing2 + practiceID + testing3 + "/";
//                        if(mapping.containsKey(entity)){
//                            mapping.get(entity).add(newPath + fileName);
//                        }
//                        else{
//                            ArrayList<String> newList = new ArrayList<String>();
//                            newList.add(newPath + fileName);
//                            mapping.put(entity, newList);
//                        }
//                    }
//                    lineCount++;
//                }
//            }
//            for(String s: mapping.keySet()){
//                if(!fs.exists(new Path(outPath + s + ".txt"))){
//                    fs.createNewFile(new Path(outPath + s + ".txt"));
//                }
//                FSDataOutputStream out = fs.append(new Path(outPath + s + ".txt"));
//                for(String link : mapping.get(s)){
//                    out.writeUTF(link + "\n");
//                }
//                out.close();
//            }
//        }
        return null;
    }


    public static void main(String[] args) throws IOException {
        testing = args[0];
        entity = args[1];


        getControlPath(args[0]);
    }




    public static void readFilesFromPath(Path pathToFiles) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(pathToFiles);



        for(FileStatus status : fileStatuses){
            if(status.isDirectory()){
                System.out.println(status.getPath().getName());
                if(status.getPath().getName().startsWith(dateWildCard)) {
                    System.out.println("DIRECTORY:" + status.getPath().toString());
                    readFilesFromPath(status.getPath());
                }
            }
            else{
                if(status.getPath().toString().contains("Manifest")){
                    manifestFiles.add(status.getPath());
                }
                else if(status.getPath().toString().toUpperCase().contains("CONTROL")){
                    controlFiles.add(status.getPath());
                }
                System.out.println("FILE:" + status.getPath().toString());
            }
        }
    }






}