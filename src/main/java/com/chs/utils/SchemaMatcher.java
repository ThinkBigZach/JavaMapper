package com.chs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.log4j.Logger;

/**
 * @author cr186034
 */

//TODO(1): Put file(path) compareURL into "error array"
public class SchemaMatcher {
	
	private static String delimiter = "\\036";
	private static Logger LOG = Logger.getLogger(SchemaMatcher.class);
	
    public static boolean matchSchemas(String goldenURL, String compareURL) throws FileNotFoundException {
        boolean tripwire = false;
        Scanner goldenFile = new Scanner(new File(goldenURL)).useDelimiter(delimiter);
        Scanner compareFile = new Scanner(new File(compareURL)).useDelimiter(delimiter);
        int golden_size = cleanStringByColumn(goldenFile.next()).length;
        int file2_size = cleanStringByColumn(compareFile.next()).length;
        //My understanding (via Gary) that RIGHT NOW, it should ONLY match when column counts are the SAME
        if (golden_size == file2_size) {
//            System.out.println("Schema matched! SUCCESS");
            LOG.info("==========Schema match success===========");
            tripwire = true;
        } else {
//            System.out.println(String.format("Comparing schema had different number of columns than Golden. \nMoving \n\t%s \nto Error Array. FAILURE",
//                    compareURL));
        	LOG.info("==========Schema match failed============");
            //TODO:1
        }
        goldenFile.close();
        compareFile.close();
        return tripwire;
    }

    private static String[] cleanStringByColumn(String beCleaned) {
        return beCleaned.replaceAll("\\036", "")
                .replaceAll("\n", "")
                .replaceAll("\\037", ",").split(",");
    }
}