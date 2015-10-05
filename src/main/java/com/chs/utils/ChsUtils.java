package com.chs.utils;

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
    
    public static String getPatternMatch(String header){
        String[] headerInfo = header.split(UNIT_SEPARATOR);
        String varcharMatch = ".*";
        String numberMatch = "[+-]?(\\d*\\.?\\d*)";
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
        return pattern;
    }
}
