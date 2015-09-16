package com.chs.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * @author cr186034
 */

//TODO:  replace replace println statments with logging statements.
//TODO: make delimineters constants.
public class SchemaMatcher {
    public static boolean matchSchemas(String goldenURL, String compareURL) throws FileNotFoundException {
        boolean tripwire = false;
        Scanner goldenFile = new Scanner(new File(goldenURL)).useDelimiter("\\036");
        Scanner compareFile = new Scanner(new File(compareURL)).useDelimiter("\\036");
        int golden_size = cleanStringByColumn(goldenFile.next()).length;
        int file2_size = cleanStringByColumn(compareFile.next()).length;
        //My understanding (via Gary) that RIGHT NOW, it should ONLY match when column counts are the SAME
        if (golden_size == file2_size) {
            System.out.println("Schema matched! SUCCESS");
            tripwire = true;
        } else {
            System.out.println(String.format("Comparing schema had different number of columns than Golden. \nMoving \n\t%s \nto Error Array. FAILURE",
                    compareURL));
            //TODO:Put file(path) compareURL into "error array"
        }
        return tripwire;
    }

    private static String[] cleanStringByColumn(String beCleaned) {
        return beCleaned.replaceAll("\\036", "")
                .replaceAll("\n", "")
                .replaceAll("\\037", ",").split(",");
    }
}