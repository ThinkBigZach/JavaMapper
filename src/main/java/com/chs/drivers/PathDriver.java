package com.chs.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class PathDriver implements Driver {

    private final String CR = "\r"; //carriage return
    private final String LF = "\n"; //line feed
    private final String UNIT_SEPARATOR = "\037";
    private final String RECORD_SEPARATOR = "\036";
	
	private String input_path;
	private String entity;
	private String inputParamEntity;
	private String out_path;
	private String entityMap_path;
	private String practiceMap_path;
	private String TD_Password;
	private String TD_Host;
	private String TD_User;
	private String TD_Database;
	private String jobID;

	public PathDriver(String[] args) {
			input_path = args[0];
			entity = args[1];
		    inputParamEntity = args[1];
			out_path = args[2];
			practiceMap_path = args[3]; 
			entityMap_path = args[4]; 
			TD_Host = args[5];
			TD_User = args[6]; 
			TD_Password = args[7]; 
			TD_Database = args[8];
	}

	public void start() {
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path("hdfs://" + input_path));
			getJobID(fs, status);
			for(FileStatus stat : status) {
				if (stat.isFile()) {
					String filename = stat.getPath().getName();
					if(!filename.equalsIgnoreCase("CONTROL.TXT")){
						String current_entity = filename.split(".")[0];
						FSDataOutputStream out = fs.append(new Path(out_path + "/" + current_entity + "/" + ".txt"));
						Scanner fileScanner = new Scanner(fs.open(stat.getPath()));
			            fileScanner.useDelimiter(RECORD_SEPARATOR);
			            String line = "";
			            int lineCount = 0;
			            while(fileScanner.hasNextLine()) {
			                line = fileScanner.next();
			                if(lineCount == 0){
//			                    TODO: Column Validation Method
			                }
			                if (lineCount > 3) {
			                    String fixedLine = replaceCRandLF(line);
			                    fixedLine = fixedLine + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobID + UNIT_SEPARATOR + filename;
			                    out.write((fixedLine + "\n").getBytes());
			                }
			                lineCount++;
			            }
			            out.close();
					}
				}
			}
		} catch (Exception e) {
//			e.printStackTrace();
			System.out.println("returnCode=FAILURE");
		} 
	}
	
    private void getJobID(FileSystem fs, FileStatus[] status) throws IOException, FileNotFoundException {
    	FileStatus stat = null;
    	boolean hasControl = false;
    	for(FileStatus s : status){
    		if(s.getPath().getName().equalsIgnoreCase("CONTROL.TXT")){
    			stat = s;
    			hasControl = true;
    		}
    	}
    	if(!hasControl){
//    		throw new FileNotFoundException("The input directory does not contain a control file");
    		System.out.println("returnCode=FAILURE");
    	}
    	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));
        String line=br.readLine();
        String[] splits = line.split("~");
        jobID = splits[4];
        br.close();		
	}

	private  String replaceCRandLF(String line){
        line = line.replaceAll(CR, "");
        line = line.replaceAll(LF, "");
        line = line.replaceAll(RECORD_SEPARATOR, "");
        return line;
    }
}
