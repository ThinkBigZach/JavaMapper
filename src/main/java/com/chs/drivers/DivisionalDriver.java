package com.chs.drivers;

import com.chs.utils.*;
import jregex.Matcher;
import jregex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.lf5.util.DateFormatManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class DivisionalDriver implements Driver {
	
	private static Logger LOG = Logger.getLogger("DivisionalDriver");

    //ascii replacement args
    private static final String UNIT_SEPARATOR = ChsUtils.UNIT_SEPARATOR;
    private static final String RECORD_SEPARATOR = ChsUtils.RECORD_SEPARATOR;

    //Constructor Args
    private String input_path;
    private String entity;
    private String inputParamEntity;
    private String out_path;
    private String practiceMap_path;
    private String entityMap_path;
    private String divisional_path;
    private String TD_Host;
    private String TD_User;
    private String TD_Password;
    private String TD_Database;
    private String regex_flag;

    //unsorted Args
    private String fileName;
    private FileSystem fs;
    private ArrayList<Path> manifestFiles;
    private ArrayList<Path> controlFiles;
    private ArrayList<String> errorArray;
    private ArrayList<String> validPracticeIDs;
    private ArrayList<String> validDivisionIDs;
    private ArrayList<String> validEntityNames;
    private HashMap<String, ArrayList<String>> mapping;


    public DivisionalDriver(String[] args) {
        input_path = args[0] + "/" + args[2];
        entity = args[1];
        inputParamEntity = args[1];
        out_path = args[3];
        practiceMap_path = args[4];
        entityMap_path = args[5];
        divisional_path = args[6];
        TD_Host = args[7];
        TD_User = args[8];
        TD_Password = args[9];
        TD_Database = args[10];
        regex_flag = args[12];
        manifestFiles = new ArrayList<Path>();
        controlFiles = new ArrayList<Path>();
        errorArray = new ArrayList<String>();
        validPracticeIDs = new ArrayList<String>();
        validEntityNames = new ArrayList<String>();
        validDivisionIDs = new ArrayList<String>();
    }

    /**
     * Reads in a wildcardable path. The division id, pratice id, and date are wildcardable
     * @param pathToControl -- This is the path to the CONTROL or the MANIFEST file
     * @throws IOException
     */
    private void getManifestPaths(String pathToControl) throws IOException {
        //MEANS A DIVISION WILDCARD
        if (pathToControl.contains("data/*")) {
            String tempPath = pathToControl.substring(0, pathToControl.indexOf("/*"));
            readDivisionalWildcard(tempPath, pathToControl.substring(pathToControl.indexOf("/*") + 2));
        }
        //FOR THE DATE WILD CARD USE CASE
        else if (pathToControl.endsWith("*")) {
            String temp = pathToControl;
            while (temp.contains("/")) {
                temp = temp.substring(temp.indexOf("/") + 1);
            }
            String dateWildCard = temp.substring(0, temp.length() - 1);
            pathToControl = pathToControl.substring(0, pathToControl.indexOf(dateWildCard));
            readDateWildCard(new Path(pathToControl), dateWildCard, true);
        } else {
            readDateWildCard(new Path(pathToControl), null, false);
        }
        removeUnusedControlFiles();
        writeOutFileLocations(manifestFiles, "Manifest");

        writeOutFileLocations(controlFiles, "Control");

    }

    /**
     * Get Manifest Paths adds too many Control.txt files. This removes all the Control.txt files that don't have a
     * corresponding Manifest file
     */
    public void removeUnusedControlFiles() {
        ArrayList<Path> newControl = new ArrayList<Path>();
        for (Path p : controlFiles) {
            String id = p.toString().substring(p.toString().indexOf("data/") + 5, p.toString().indexOf("/athena"));
            for (Path path : manifestFiles) {
                if (path.toString().contains(id)) {
                    newControl.add(p);
                    break;
                }
            }
        }
        controlFiles.clear();
        for (Path p : newControl) {
            controlFiles.add(p);
        }
    }


    /**
     * Reads in all the entity.asv files and consolidates them into 1 entity file
     * It also removes carriage returns and line feed characters and nulls out personally identifiable information
     * @param paths -- List of paths for a given entity
     * @param entity -- The entity that the method is writing a file for
     * @throws IOException
     */
    private void readAndLoadEntities(ArrayList<String> paths, String entity) throws IOException {
        LOG.info("WRITING FILE FOR ENTITY " + entity);
        String entityOutpath = out_path + "/" + entity.toLowerCase() + "/";
        String outFileNameMili = ChsUtils.appendTimeAndExtension(entityOutpath + entity);
        String errOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/" + entity.toLowerCase() + "/";
        String errFileNameMili = ChsUtils.appendTimeAndExtension(errOutpath + entity);

        if (!fs.exists(new Path(entityOutpath))) {
            fs.mkdirs(new Path(entityOutpath));
        }
        if (!fs.exists(new Path(outFileNameMili))) {
            fs.createNewFile(new Path(outFileNameMili));
        }
        FSDataOutputStream out = fs.append(new Path(outFileNameMili));
        FSDataOutputStream err = null;
        for (String path : paths) {
            String jobId = getJobIdFromPaths(path);
            String myFileName = path.substring(path.lastIndexOf("/") + 1);
            Scanner fileScanner = new Scanner(fs.open(new Path(path)));
            fileScanner.useDelimiter(RECORD_SEPARATOR);
            String line;
            int lineCount = 0;
            String headerTypes;
            ArrayList<Integer> numericColumnIndices = new ArrayList<Integer>();
            String headerInfo = null;
            Map<String, List<SchemaRecord>> schemas = SchemaMatcher.goldenEntitySchemaMap;
            boolean needsProcess = PiiObfuscator.hasRemoveComment(schemas.get(entity.toLowerCase()));
            boolean needsReorder = false;// = needsDynamicSchemaReorder(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), headerInfo.split(UNIT_SEPARATOR))
            boolean needsRegex = false;
            while (fileScanner.hasNextLine()) {

                line = fileScanner.next();
                if (lineCount == 0) {
                    headerInfo = line;

                    needsReorder = ChsUtils.needsDynamicSchemaReorder(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), headerInfo);
                    if (!ChsUtils.validateColumnCounts(entity, new Path(path).toString(), fs)) {
                        errorArray.add(path);
                        break;
                    }
                }
                if (lineCount == 1) {
                    headerTypes = line;
                    numericColumnIndices = ChsUtils.getNumberIndices(headerTypes);
                    if (regex_flag.equalsIgnoreCase("validate")) {
                        needsRegex = true;
                    }
                }
                if (lineCount > 3 && line.trim().length() > 0) {
                    String cleanLine = ChsUtils.replaceCRandLF(line);
                    String cline = cleanLine;
                    cleanLine = cleanLine + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + myFileName;
                    if (needsProcess) {
                        cleanLine = PiiObfuscator.piiProcess(cleanLine.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR), schemas.get(entity.toLowerCase()), UNIT_SEPARATOR);
                    }
                    boolean isGoodLine = true;
                    if (needsRegex) {
                        isGoodLine = ChsUtils.matchNumberTypes(cleanLine, numericColumnIndices);
                    }
                    int cl_int = cleanLine.split(UNIT_SEPARATOR).length; //Splitter.on(UNIT_SEPARATOR).splitToList(cleanLine).size();
                    int he_int = headerInfo.split(UNIT_SEPARATOR).length;//Splitter.on(UNIT_SEPARATOR).splitToList(headerInfo).size();
                    if (cl_int == he_int + 3 && isGoodLine) {
                        String lineclone = cline;
                        if (needsReorder) {
                            lineclone = ChsUtils.reorderAlongSchema(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), cline.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR));
                        }
                        lineclone = lineclone + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + myFileName;
                        out.write((lineclone + "\n").getBytes());
                    } else {
                        if (!fs.exists(new Path(errOutpath))) {
                            fs.mkdirs(new Path(errOutpath));
                        }
                        if (!fs.exists(new Path(errFileNameMili))) {
                            fs.createNewFile(new Path(errFileNameMili));
                            err = fs.append(new Path(errFileNameMili));
                        } else if (err == null) {
                            err = fs.append(new Path(errFileNameMili));
                        }
                        err.write((cleanLine + "\n").getBytes());
                    }
                }
                lineCount++;
            }
        }
        if (err != null) {
            err.close();
        }
        out.close();
    }


    /**
     * Gets the JobID for a given path
     * @param path -- The path to look for
     * @return
     */
    private String getJobIdFromPaths(String path) {
        String temp = path.substring(0, path.lastIndexOf('/'));
        temp = temp + "/CONTROL.TXT";

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(temp))));
            String line = br.readLine();
            if (!line.isEmpty()) {
                return line.split("~")[3];
            }
        } catch (IOException e) {
        	LOG.warn(e.getMessage());
//            e.printStackTrace();
//        	System.out.println("returnCode=FAILURE");
        }
        return "";

    }



    /**
     * Checks if the practice id is valid and the entity is valid
     * @param practiceID
     * @param entityName
     * @return
     */
    private boolean isValidEntry(String practiceID, String entityName) {
        return isValidPractice(practiceID) && isValidEntity(entityName);
    }


    /**
     * Checks if the division is valid
     * @param divisionID
     * @return
     */
    private boolean isValidDivision(String divisionID) {
        return validDivisionIDs.contains(divisionID);
    }


    /**
     * Checks if the practice id is valid
     * @param practiceID
     * @return
     */
    private boolean isValidPractice(String practiceID) {
        boolean isValid = validPracticeIDs.contains(practiceID.toUpperCase());
        return isValid;
    }


    /**
     * Checks if the entity is valid
     * @param entityName
     * @return
     */
    private boolean isValidEntity(String entityName) {
        boolean isValid = validEntityNames.contains(entityName.toUpperCase());
        return isValid;
    }


    /**
     * Takes in the path for a Manifest file. It replaces the division id with the practice id
     * Which is the new path for the entities
     * @param path -- Path to the Manifest file
     * @param practiceID -- Practice id to switch
     * @return
     */
    private String generateNewPath(Path path, String practiceID) {
        String fixedPath = path.toString().substring(0, path.toString().indexOf("Manifest"));
        String testing2 = fixedPath.substring(0, fixedPath.indexOf("/data/") + 6);
        String testing3 = fixedPath.substring(fixedPath.indexOf("/athena/"));
        String newPath = testing2 + practiceID + testing3;
        return newPath;
    }


    /**
     * Loads all the entity files into a map at processLine(p, line)
     * Writes out the Manifest and Control files if the input entity is *
     * @param files
     * @param type
     * @throws IOException
     */
    private void writeOutFileLocations(ArrayList<Path> files, String type) throws IOException {
        String manconOutpath = out_path + "/" + type.toLowerCase() + "/" + type;
        String outFileNameMili = ChsUtils.appendTimeAndExtension(manconOutpath);
        if (!fs.exists(new Path(outFileNameMili))) {
            fs.createNewFile(new Path(outFileNameMili));
        }
        String myFile = "";
        for (Path p : files) {
            String jobId = getJobIdFromPaths(p.toString());
            Scanner fileScanner = new Scanner(fs.open(p));
            fileScanner.useDelimiter(RECORD_SEPARATOR);
            String line = "";
            int current_line = 0;

            while (fileScanner.hasNextLine()) {
                line = fileScanner.next();

                if (current_line > 3 && type.equalsIgnoreCase("MANIFEST")) {
                    processLine(p, line);
                    myFile += ChsUtils.replaceCRandLF(line) + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + p.getName() + "\n";
                } else if (type.equalsIgnoreCase("CONTROL")) {
                    String fixedLine = ChsUtils.replaceCRandLF(line);
                    fixedLine = fixedLine.replaceAll("~", UNIT_SEPARATOR);
                    myFile += fixedLine + "\n";
                }
                current_line++;
            }
        }

        if (inputParamEntity.equalsIgnoreCase("*")) {
            FSDataOutputStream out = fs.append(new Path(outFileNameMili));
            out.write(myFile.getBytes());
            out.close();
        }
    }

    /**
     * Takes a path and a line
     * Get the practice id from the line
     * replaces the division id in the path with the practice id from the line
     * Adds the new path to the entity mapping
     * @param p
     * @param line
     */
    private void processLine(Path p, String line) {
        String practiceID;
        line = line.replaceFirst("^\\s+", "");
        line = ChsUtils.replaceCRandLF(line);
        String splitValue[] = line.split(UNIT_SEPARATOR);
        if (splitValue.length > 1) {

            if (Integer.parseInt(splitValue[1]) > 0) {
                practiceID = line.substring(0, line.indexOf(".asv"));
                fileName = line.substring(0, line.indexOf(".asv") + 4);
                entity = line.substring(0, line.indexOf("_"));
                while (practiceID.contains("_")) {
                    practiceID = practiceID.substring(practiceID.indexOf("_") + 1);
                }
                String newPath = generateNewPath(p, practiceID);
                if (isValidEntry(practiceID, entity)) {
                    addToMapping(newPath);

                    //Creates a path for CONTROL.TXT in each Practice ID Folder
                    Path controlPath = new Path(newPath + "CONTROL.TXT");
                    if (!controlFiles.contains(controlPath)) {
                        controlFiles.add(controlPath);
                    }
                }
            }
        }
    }


    /**
     * Takes a new path and adds it to the mapping for later processing
     * @param newPath
     */
    private void addToMapping(String newPath) {
        if (mapping.containsKey(entity.toUpperCase())) {
            mapping.get(entity.toUpperCase()).add(newPath + fileName);
        } else {
            ArrayList<String> newList = new ArrayList<String>();
            newList.add(newPath + fileName);
            mapping.put(entity.toUpperCase(), newList);
        }
    }


    /**
     * Takes in a path that ends with a date wildcard, it then recursively walks down the path
     * @param pathToFiles -- the path up to the date wildcard (e.g /user/financialDataFeed/data/athena/finished/)
     * @param dateWildCard -- the piece of the date wildcard (e.g. 2015-08-1*)
     * @param wildCarded -- indicator if it's wildcarded or not
     * @throws IOException
     */
    private void readDateWildCard(Path pathToFiles, String dateWildCard, boolean wildCarded) throws IOException {
        String divisionId = pathToFiles.toString().substring(pathToFiles.toString().indexOf("/data/") + 6);
        divisionId = divisionId.substring(0, divisionId.indexOf("/"));
        if (fs.exists(pathToFiles) && isValidDivision(divisionId)) {
            FileStatus[] fileStatuses = fs.listStatus(pathToFiles);
            for (FileStatus status : fileStatuses) {
                if (status.isDirectory()) {
                    if (wildCarded) {
                        if (status.getPath().getName().startsWith(dateWildCard)) {
                            readDateWildCard(status.getPath(), dateWildCard, false);
                        }
                    } else {
                        readDateWildCard(status.getPath(), dateWildCard, false);
                    }
                } else {
                    if (status.getPath().toString().toUpperCase().contains("MANIFEST")) {
                        manifestFiles.add(status.getPath());
                    } else if (status.getPath().toString().toUpperCase().contains("CONTROL")) {
                        controlFiles.add(status.getPath());
                    }
                }
            }
        }
    }


    /**
     * Reads a path with a divisional wildcard (e.g. /user/financialDataFeed/data/* athena/finished/2015-09-01
     * @param divisionPart -- the part before the wildcard  /user/financialDataFeed/data/*
     * @param datePart -- the part after the wildcard /athena/finished/2015-09-01
     * @throws IOException
     */
    private void readDivisionalWildcard(String divisionPart, String datePart) throws IOException {
        //FIRST READ IN ALL DIVISION FOLDERS
        FileStatus[] fileStatuses = fs.listStatus(new Path(divisionPart));
//        System.out.println(divisionPart);

        for (FileStatus status : fileStatuses) {

            if (status.isDirectory() && isValidDivision(status.getPath().getName())) {

                //Reads the directory and appends the date part
                String temp = status.getPath().toString() + datePart;

                if (temp.endsWith("*")) {

                    //DATE IS WILDCARDED TOO
                    String dateWildCard = temp.substring(temp.lastIndexOf("/") + 1);
                    dateWildCard = dateWildCard.substring(0, dateWildCard.length() - 1);

                    readDateWildCard(new Path(temp.substring(0, temp.indexOf(dateWildCard))), dateWildCard, true);
                } else {

                    //DATE ISN'T WILDCARDED READ NORMALLY
                    try {
                        FileStatus[] dateFiles = fs.listStatus(new Path(temp));
                        for (FileStatus dateStatus : dateFiles) {
                            if (dateStatus.getPath().toString().toUpperCase().contains("MANIFEST")) {
                                manifestFiles.add(dateStatus.getPath());
                            } else if (dateStatus.getPath().toString().toUpperCase().contains("CONTROL")) {
                                controlFiles.add(dateStatus.getPath());
                            }
                        }
                    } catch (Exception e) {
                    	LOG.warn("PATH " + temp + " does not exist!");
//                    	System.out.println("PATH " + temp + "Does not exist!");
//                    	System.out.println("returnCode=FAILURE");
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.chs.drivers.Driver#start()
     * core launch method. All logic for divisional load needs to be in or called from this method
     */

    /**
     * The start method from the Driver interface used to execute a given job
     */
    public void start() {
//        System.out.println("CURRENT TIME IN MILLIS IS:" + System.currentTimeMillis());
    	LOG.info("CURRENT TIME IN MILLIS IS: " + System.currentTimeMillis());
        long startTime = System.currentTimeMillis();
        TDConnector.init(TD_Host, TD_User, TD_Password, TD_Database);
        TDConnector.getConnection();
        try {
            fs = FileSystem.newInstance(new Configuration());
            mapping = new HashMap<String, ArrayList<String>>();
            ChsUtils.getValidEntityNames(entityMap_path, out_path, validEntityNames, fs);
            ChsUtils.getValidPracticeIds(practiceMap_path, validPracticeIDs, fs);
            ChsUtils.getValidDivisionIds(divisional_path, validDivisionIDs, fs);
            LOG.info("GOT ENTITIES AND PRACTICES");
            this.getManifestPaths(input_path);
            LOG.info("GOT MANIFEST PATHS");
            try {
                long startWrite = System.currentTimeMillis();
                //TODO: This can be threaded to somehow work with the readAndLoadEntities
                for (String s : mapping.keySet()) {
                    if (s.equalsIgnoreCase(inputParamEntity) || inputParamEntity.equalsIgnoreCase("*")) {
                        this.readAndLoadEntities(mapping.get(s), s);
                    }
                }
                long endWrite = System.currentTimeMillis();
//                System.out.println(((endWrite - startWrite)/1000) + " seconds to execute writing the files");

            } catch (Exception e) {
                e.printStackTrace();
                LOG.fatal(e.getMessage());
                System.out.println("returnCode=FAILURE");
            }
            long endTime = System.currentTimeMillis();
//            System.out.println(((endTime - startTime)/1000) + " seconds to execute entire request");
            LOG.info(((endTime - startTime)/1000) + " seconds to execute entire request");
            writeErrorFiles();
        } catch (IOException e) {
            e.printStackTrace();
            LOG.fatal(e.getMessage());
            System.out.println("returnCode=FAILURE");
        }
    }


    /**
     * Writes out all the files that had errors in them
     * @throws IllegalArgumentException
     * @throws IOException
     */
    private void writeErrorFiles() throws IllegalArgumentException, IOException {
        String tempOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/";
        if (!fs.exists(new Path(tempOutpath))) {
            fs.mkdirs(new Path(tempOutpath));
        }
        if (errorArray.size() > 0) {
            Date tempDate = new Date(System.currentTimeMillis());
            String yyyymmddhhmmss = new DateFormatManager().format(tempDate, "yyyy-MM-dd-hh-mm-ss");
            String errorOutpath = tempOutpath + "error-" + yyyymmddhhmmss + ".txt";
            if (!fs.exists(new Path(errorOutpath))) {
                fs.createNewFile(new Path(errorOutpath));
            }
            FSDataOutputStream out = fs.append(new Path(errorOutpath));
            for (String line : errorArray) {
                out.write((line).getBytes());
            }
            out.close();
        }
    }

}