package com.chs.utils;

import org.apache.hadoop.fs.Path;

import java.util.List;

import com.google.common.base.Splitter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;


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
        List<String> headerInfo = Splitter.on(UNIT_SEPARATOR).splitToList(header);
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
}
