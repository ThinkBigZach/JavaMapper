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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class PathDriver implements Driver {

	private static Logger LOG = Logger.getLogger("PathDriver");
	private static final String UNIT_SEPARATOR = ChsUtils.UNIT_SEPARATOR;
	private static final String RECORD_SEPARATOR = ChsUtils.RECORD_SEPARATOR;
	private ArrayList<String> validPracticeIDs;
	private ArrayList<String> validEntityNames;
	private ArrayList<String> errorArray;
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
	private String regex_flag;

	public PathDriver(String[] args) {
		input_path = args[0];
		entity = args[1];
		inputParamEntity = args[1];
		out_path = args[2];
		practiceMap_path = args[3]; 
		entityMap_path = args[4]; 
		TD_Host = args[6];
		TD_User = args[7]; 
		TD_Password = args[8]; 
		TD_Database = args[9];
		regex_flag = args[11];
		validPracticeIDs = new ArrayList<String>();
		validEntityNames = new ArrayList<String>();
		errorArray = new ArrayList<String>();
		FileSystem fs;
		try {
			fs = FileSystem.newInstance(new Configuration());
			ChsUtils.getValidEntityNames(entityMap_path, out_path, validEntityNames, fs);
			ChsUtils.getValidPracticeIds(practiceMap_path, validPracticeIDs, fs);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.fatal(e.getMessage());
		}

	}

	public void start() {
		FileSystem fs;
		TDConnector.init(TD_Host, TD_User, TD_Password, TD_Database);
        TDConnector.getConnection();
		try {
			fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(input_path));
			getJobID(fs, status);
			for(FileStatus stat : status) {
				if (stat.isFile()) {
					String filename = stat.getPath().getName();
					if(!filename.equalsIgnoreCase("CONTROL.TXT") && (filename.contains(entity.toLowerCase()) || entity.equals("*"))){
						writeEntity(entity, fs, jobID, stat.getPath(), filename);
					}
				}
			}
			writeErrorFiles(fs);
		} catch (Exception e) {
			//			e.printStackTrace();
			LOG.fatal(e.getMessage());
			System.out.println("returnCode=FAILURE");
		} 
	}

	private void writeEntity(String entity, FileSystem fs, String jobId, Path path, String filename) throws IOException {
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
		while(fileScanner.hasNextLine()) {
			line = fileScanner.next();
			if(lineCount == 0){
				headerInfo = line;

				needsReorder = ChsUtils.needsDynamicSchemaReorder(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), headerInfo);
				if (!ChsUtils.validateColumnCounts(entity, path.toString(), fs))
				{
					errorArray.add(path.toString());
					break;
				}
			}
			else if(lineCount == 1){
				headerTypes = line;
				numericColumnIndices = ChsUtils.getNumberIndices(headerTypes);
				if(regex_flag.equalsIgnoreCase("validate")) {
					needsRegex = true;
				}
			}
			else if (lineCount > 3 && line.trim().length() > 0) {
				String cleanLine = ChsUtils.replaceCRandLF(line);
				if (needsProcess)
				{
					cleanLine = PiiObfuscator.piiProcess(cleanLine.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR), schemas.get(entity.toLowerCase()), UNIT_SEPARATOR);
				}
				boolean isGoodLine = true;
				if(needsRegex){
					isGoodLine = ChsUtils.matchNumberTypes(cleanLine, numericColumnIndices);
				}               	
				String cline = cleanLine;
				cleanLine = cleanLine + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + filename;
				int cl_int = cleanLine.split(UNIT_SEPARATOR).length; //Splitter.on(UNIT_SEPARATOR).splitToList(cleanLine).size();
				int he_int = headerInfo.split(UNIT_SEPARATOR).length;//Splitter.on(UNIT_SEPARATOR).splitToList(headerInfo).size();
				if(cl_int == he_int + 3 && isGoodLine) {
					String lineclone = cline;
					if (needsReorder)
					{
						lineclone = ChsUtils.reorderAlongSchema(SchemaMatcher.getOrderingSchema(entity.toLowerCase()), cline.split(UNIT_SEPARATOR), headerInfo.split(UNIT_SEPARATOR));                			
					}
					lineclone = lineclone + UNIT_SEPARATOR + "0" + UNIT_SEPARATOR + jobId + UNIT_SEPARATOR + filename;
					//                		System.out.println(String.format("BEFORE: \n\t%s \nAFTER: \n\t%s", cleanLine, lineclone));
					out.write((lineclone + "\n").getBytes());
				}
				else {
					if(!fs.exists(new Path(errOutpath))){
						fs.mkdirs(new Path(errOutpath));
					}
					if(!fs.exists(new Path(errFileNameMili))){
						fs.createNewFile(new Path(errFileNameMili));
						err = fs.append(new Path(errFileNameMili));
					}
					else if(err == null){
						err = fs.append(new Path(errFileNameMili));
					}
					err.write((cleanLine + "\n").getBytes());
				}
			}
			lineCount++;
		}
		if(err != null){
			err.close();
		}
		out.close();
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
