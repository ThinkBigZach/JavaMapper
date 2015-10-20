package com.chs.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.lf5.util.DateFormatManager;

import com.chs.utils.ChsUtils;
import com.chs.utils.PiiObfuscator;
import com.chs.utils.SchemaMatcher;
import com.chs.utils.SchemaRecord;
import com.chs.utils.TDConnector;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PathDriver implements Driver {

	private static Logger LOG = Logger.getLogger("PathDriver");
	private static final String UNIT_SEPARATOR = ChsUtils.UNIT_SEPARATOR;
	private static final String RECORD_SEPARATOR = ChsUtils.RECORD_SEPARATOR;
	private ArrayList<String> validPracticeIDs;
	private ArrayList<String> validEntityNames;
	private ArrayList<String> validDivisionIDs;
	private ArrayList<String> errorArray;
	private String input_path;
	private String entity;
	private String out_path;
	private String entityMap_path;
	private String practiceMap_path;
	private String divisionMap_path;
	private String TD_Password;
	private String TD_Host;
	private String TD_User;
	private String TD_Database;
	private String jobID;
	private String regex_flag;
	private FileSystem fs;
	private HashMap<String, List<Path>> mapping;

	public PathDriver(String[] args) {
		input_path = args[0] + "/" + args[2];
		entity = args[1];
		out_path = args[3];
		practiceMap_path = args[4]; 
		entityMap_path = args[5];
		divisionMap_path = args[6];
		TD_Host = args[7];
		TD_User = args[8]; 
		TD_Password = args[9]; 
		TD_Database = args[10];
		regex_flag = args[12];
		validPracticeIDs = new ArrayList<String>();
		validEntityNames = new ArrayList<String>();
		validDivisionIDs = new ArrayList<String>();
		errorArray = new ArrayList<String>();
		mapping = new HashMap<String, List<Path>>();
		try {
			fs = FileSystem.newInstance(new Configuration());
			ChsUtils.getValidEntityNames(entityMap_path, out_path, validEntityNames, fs);
			ChsUtils.getValidPracticeIds(practiceMap_path, validPracticeIDs, fs);
			ChsUtils.getValidDivisionIds(divisionMap_path, validDivisionIDs, fs);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.fatal(e.getMessage());
		}

	}

	/**
	 * Reads in a wildcardable path. The division id, pratice id, and date are wildcardable
	 * @param pathToControl -- This is the path to the CONTROL or the MANIFEST file
	 * @throws IOException
	 */
	private void readInputPath(String pathToControl) throws IOException {
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
	}


	/**
	 * Checks if the division is valid
	 * @param divisionID
	 * @return
	 */
	private boolean isValidDivision(String divisionID) {
		return validDivisionIDs.contains(divisionID);
	}
	private boolean isValidPractice(String practice) {
		return validPracticeIDs.contains(practice);
	}
	private void readDateWildCard(Path pathToFiles, String dateWildCard, boolean wildCarded) throws IOException {
		String divisionId = pathToFiles.toString().substring(pathToFiles.toString().indexOf("/data/") + 6);

		divisionId = divisionId.substring(0, divisionId.indexOf("/"));
		Map<String, List<SchemaRecord>> records = SchemaMatcher.goldenEntitySchemaMap;
		if (fs.exists(pathToFiles) && isValidPractice(divisionId)) {
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
				}
				else {
					if(status.getPath().toString().toUpperCase().contains("MANIFEST") || status.getPath().toString().toUpperCase().contains("CONTROL")) {
						getJobID(fs, fileStatuses);
					}
					else{
						String filename = status.getPath().getName();
						filename = filename.substring(0, filename.indexOf("_"));
						if(isValidEntity(filename)) {
							if (mapping.containsKey(filename)) {
								mapping.get(filename).add(status.getPath());
							} else {
								ArrayList<Path> newPath = new ArrayList<Path>();
								newPath.add(status.getPath());
								mapping.put(filename, newPath);
							}
						}
					}
				}
			}
		}
	}

	private void readDivisionalWildcard(String divisionPart, String datePart) throws IOException {
		//FIRST READ IN ALL DIVISION FOLDERS
		FileStatus[] fileStatuses = fs.listStatus(new Path(divisionPart));
//        System.out.println(divisionPart);
		for (FileStatus status : fileStatuses) {
			if (status.isDirectory() ) {
				if(isValidPractice(status.getPath().getName())){
					//Reads the directory and appends the date part
					String temp = status.getPath().toString() + datePart;

					if (temp.endsWith("*")) {

						//DATE IS WILDCARDED TOO
						String dateWildCard = temp.substring(temp.lastIndexOf("/") + 1);
						dateWildCard = dateWildCard.substring(0, dateWildCard.length() - 1);

						readDateWildCard(new Path(temp.substring(0, temp.indexOf(dateWildCard))), dateWildCard, true);
					} else {
						String dateWildCard = temp.substring(temp.lastIndexOf("/") + 1);
						readDateWildCard(new Path(temp), dateWildCard, false);
						//DATE ISN'T WILDCARDED READ NORMALLY
					}
				}
			}
			else{
				if(status.getPath().toString().toUpperCase().contains("MANIFEST") || status.getPath().toString().toUpperCase().contains("CONTROL")) {

				}
				else{
					String filename = status.getPath().getName();
					System.out.println(filename);
					filename = filename.substring(0, filename.indexOf("_"));
					if(isValidEntity(filename)) {
						if (mapping.containsKey(filename)) {
							mapping.get(filename).add(status.getPath());
						} else {
							ArrayList<Path> newPath = new ArrayList<Path>();
							newPath.add(status.getPath());
							mapping.put(filename, newPath);
						}
					}
				}
			}
		}
	}

	private boolean isValidEntity(String entityName) {
		return validEntityNames.contains(entityName.toUpperCase());
	}
	public void start() {
		FileSystem fs;
		TDConnector.init(TD_Host, TD_User, TD_Password, TD_Database);
        TDConnector.getConnection();
		try {
			fs = FileSystem.get(new Configuration());
			readInputPath(input_path);
			for(String filename: mapping.keySet()){
				System.out.println("Entity=" + filename);
				if(filename.equals(entity.toLowerCase()) || entity.equals("*")){
						System.out.println("PATH=" + mapping.get(filename).get(0).toString());
						writeEntity(filename, fs, jobID, mapping.get(filename));
				}
			}
			writeErrorFiles(fs);
		} catch (Exception e) {
						e.printStackTrace();
			LOG.fatal(e.getMessage());
			System.out.println("returnCode=FAILURE");
		} 
	}

	private void writeEntity(String entity, FileSystem fs, String jobId, List<Path> paths) throws IOException {
		LOG.info("WRITING FILE FOR ENTITY " + entity);
		String entityOutpath = out_path + "/" + entity.toLowerCase() + "/";
		String outFileNameMili = ChsUtils.appendTimeAndExtension(entityOutpath + entity);
		String errOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/" + entity.toLowerCase() + "/";
		String errFileNameMili = ChsUtils.appendTimeAndExtension(errOutpath + entity);

		if(!fs.exists(new Path(entityOutpath))){
			fs.mkdirs(new Path(entityOutpath));
		}
		if(!fs.exists(new Path(outFileNameMili))){
			fs.createNewFile(new Path(outFileNameMili));
		}
		FSDataOutputStream out = fs.append(new Path(outFileNameMili));
		FSDataOutputStream err = null;
		for(Path path : paths) {
			Scanner fileScanner = new Scanner(fs.open(path));
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
					if (!ChsUtils.validateColumnCounts(entity, path.toString(), fs)) {
						errorArray.add(path.toString());
						break;
					}
				} else if (lineCount == 1) {
					headerTypes = line;
					numericColumnIndices = ChsUtils.getNumberIndices(headerTypes);
					if (regex_flag.equalsIgnoreCase("validate")) {
						needsRegex = true;
					}
				} else if (lineCount > 3 && line.trim().length() > 0) {
					String cleanLine = ChsUtils.replaceCRandLF(line);
					if (needsProcess) {
						cleanLine = PiiObfuscator.piiProcess(cleanLine.split(UNIT_SEPARATOR, -1), headerInfo.split(UNIT_SEPARATOR), schemas.get(entity.toLowerCase()), UNIT_SEPARATOR);
					}
					boolean isGoodLine = true;
					if (needsRegex) {
						isGoodLine = ChsUtils.matchNumberTypes(cleanLine, numericColumnIndices);
					}
					String cline = cleanLine;
					cleanLine = cleanLine + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + path.getName();
					int cl_int = cleanLine.split(UNIT_SEPARATOR).length; //Splitter.on(UNIT_SEPARATOR).splitToList(cleanLine).size();
					int he_int = headerInfo.split(UNIT_SEPARATOR).length;//Splitter.on(UNIT_SEPARATOR).splitToList(headerInfo).size();
					if (cl_int == he_int + 3 && isGoodLine) {
						String lineclone = cline;
						if (needsReorder) {
							lineclone = ChsUtils.reorderAlongSchema(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), cline.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR));
						}
						lineclone = lineclone + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + path.getName();
						//                		System.out.println(String.format("BEFORE: \n\t%s \nAFTER: \n\t%s", cleanLine, lineclone));
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
		out.close();
		if (err != null) {
			err.close();
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
			LOG.warn("The input directory does not contain a control file");
			System.out.println("returnCode=FAILURE");
			//    		throw new FileNotFoundException("The input directory does not contain a control file");
		}
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));
		String line=br.readLine();
		String[] splits = line.split("~");
		jobID = splits[4];
		br.close();		
	}

	private void writeErrorFiles(FileSystem fs) throws IllegalArgumentException, IOException {
    	String tempOutpath = out_path.substring(0, out_path.lastIndexOf('/')) + "/error/";
        if (!fs.exists(new Path(tempOutpath))) {
            fs.mkdirs(new Path(tempOutpath));
        }
        if(errorArray.size() > 0) {
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
