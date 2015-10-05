package com.chs.drivers;

import com.chs.utils.ChsUtils;
import com.chs.utils.PiiObfuscator;
import com.chs.utils.SchemaRecord;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DivisionalDriverTest {


    private String[] args = {"/user/financialDataFeed/data/*/athena/finished/2015-09-01",
            "allergy",
            "/user/athena/data/financialdatafeed/finished/",
            "/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv",
            "/enterprise/mappings/athena/athena_table_defs.csv",
            "/enterprise/mappings/division_ids.csv",
            "dev.teradata.chs.net",
            "dbc",
            "dbc",
            "EDW_ATHENA_STAGE divisional"};

    private DivisionalDriver divisionalDriver = new DivisionalDriver(args);
    Class<?> divisionalDriverClass = divisionalDriver.getClass();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testReorderAlongSchema() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
    	String UNIT_SEPARATOR = "\037";
    	String header = "Allergy ID" + UNIT_SEPARATOR + "Type" + UNIT_SEPARATOR + "Patient ID" + UNIT_SEPARATOR + "Chart ID" + UNIT_SEPARATOR + 
				"CONTEXT_ID" + UNIT_SEPARATOR + "CONTEXT_NAME" + UNIT_SEPARATOR + "CONTEXT_PARENTCONTEXTID" + UNIT_SEPARATOR + "Allergy Name" + UNIT_SEPARATOR + "Allergy Code";
		String columns = "6574" + UNIT_SEPARATOR + "DT" + UNIT_SEPARATOR + "8928" + UNIT_SEPARATOR + "32" + UNIT_SEPARATOR + "24452" + UNIT_SEPARATOR + "LADIDADI"
				 + UNIT_SEPARATOR + "DATSIK" + UNIT_SEPARATOR + "Gatorade" + UNIT_SEPARATOR + "520000";
		String correctOutput = "24452" + UNIT_SEPARATOR + "LADIDADI" + UNIT_SEPARATOR + "DATSIK" + UNIT_SEPARATOR + "8928" + UNIT_SEPARATOR + "32" + UNIT_SEPARATOR + "DT"
				 + UNIT_SEPARATOR + "6574" + UNIT_SEPARATOR + "Gatorade" + UNIT_SEPARATOR + "520000";
		Map<String,Integer> goldMap = new LinkedHashMap<String,Integer>();
		goldMap.put("context_id", 0);
		goldMap.put("context_name", 1);
		goldMap.put("context_parentcontextid", 2);
		goldMap.put("patient_id", 3);
		goldMap.put("chart_id", 4);
		goldMap.put("type", 5);
		goldMap.put("allergy_id", 6);
		goldMap.put("allergy_name", 7);
		goldMap.put("allergy_code", 8);
    	Method methodReorderAlongSchema = DivisionalDriver.class.getDeclaredMethod("reorderAlongSchema", Map.class, String[].class, String[].class);
    	methodReorderAlongSchema.setAccessible(true);
    	String out = (String) methodReorderAlongSchema.invoke(divisionalDriver, goldMap, columns.split(UNIT_SEPARATOR), header.split(UNIT_SEPARATOR));
    	assertTrue(correctOutput.equals(out));
    }
    
    @Test
    public void testReorderAlongSchema_ForMismatch() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
    	String UNIT_SEPARATOR = "\037";
    	String header = "Allergy ID" + UNIT_SEPARATOR + "Type" + UNIT_SEPARATOR + "Patient ID";
    	String columns = "6574" + UNIT_SEPARATOR + "DT" + UNIT_SEPARATOR + "000";
    	String correctout = "000" + UNIT_SEPARATOR + "6574" + UNIT_SEPARATOR + "DT";
    	Map<String,Integer> goldMap = new LinkedHashMap<String,Integer>();
    	goldMap.put("patient_id", 0);
    	goldMap.put("allergy_id", 1);
    	goldMap.put("tyep", 2);
    	Method methodReorderAlongSchema = DivisionalDriver.class.getDeclaredMethod("reorderAlongSchema", Map.class, String[].class, String[].class);
    	methodReorderAlongSchema.setAccessible(true);
    	String out = (String) methodReorderAlongSchema.invoke(divisionalDriver, goldMap, columns.split(UNIT_SEPARATOR), header.split(UNIT_SEPARATOR));
    	assertNotEquals(out, correctout);
    }
    
    @Test
    public void testNeedsDynamicSchemaReorder() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
    	String UNIT_SEPARATOR = "\037";
    	String header = "Allergy ID" + UNIT_SEPARATOR + "Type" + UNIT_SEPARATOR + "Patient ID" + UNIT_SEPARATOR + "Chart ID";
    	String columns = "6574" + UNIT_SEPARATOR + "DT" + UNIT_SEPARATOR + "8928" + UNIT_SEPARATOR + "32";
    	Map<String,Integer> goldMap = new LinkedHashMap<String,Integer>();
    	goldMap.put("allergy_id", 0);
		goldMap.put("type", 1);
		goldMap.put("patient_id", 2);
		goldMap.put("chart_id", 3);
		//String[] cols = columns.split(UNIT_SEPARATOR);
		String[] heads = header.split(UNIT_SEPARATOR);
		Method methodNeedsDynamicSchemaReorder = DivisionalDriver.class.getDeclaredMethod("needsDynamicSchemaReorder", Map.class, String[].class);
		methodNeedsDynamicSchemaReorder.setAccessible(true);
		boolean out = (Boolean) methodNeedsDynamicSchemaReorder.invoke(divisionalDriver, goldMap, heads);
		assertFalse(out);
    }
    
    @Test
    public void testNeedsDynamicSchemaReorder_Needed() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
    	String UNIT_SEPARATOR = "\037";
    	String header = "Allergy ID" + UNIT_SEPARATOR + "Type" + UNIT_SEPARATOR + "Patient ID" + UNIT_SEPARATOR + "Chart ID";
    	String columns = "6574" + UNIT_SEPARATOR + "DT" + UNIT_SEPARATOR + "8928" + UNIT_SEPARATOR + "32";
    	Map<String,Integer> goldMapReorder = new LinkedHashMap<String,Integer>();
    	goldMapReorder.put("patient_id", 0);
    	goldMapReorder.put("allergy_id", 1);
    	goldMapReorder.put("type", 2);
    	goldMapReorder.put("chart_id", 3);
    	String[] heads = header.split(UNIT_SEPARATOR);
		Method methodNeedsDynamicSchemaReorder = DivisionalDriver.class.getDeclaredMethod("needsDynamicSchemaReorder", Map.class, String[].class);
		methodNeedsDynamicSchemaReorder.setAccessible(true);
		boolean out = (Boolean) methodNeedsDynamicSchemaReorder.invoke(divisionalDriver, goldMapReorder, heads);
		assertTrue(out);
    }

    @Test
    public void testProcessPii() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException{
        //Tests when working correctly
        String[] line = {"data", "data", "PIIDATA"};
        String[] header = {"col1", "col2", "piiCol"};
        String UNIT_SEPARATOR = "\037";
        ArrayList<SchemaRecord> records = new ArrayList<SchemaRecord>();
        records.add(new SchemaRecord("col1", "test", ""));
        records.add(new SchemaRecord("col2", "test", ""));
        records.add(new SchemaRecord("piiCol", "test", "remove"));

        String returnVal = PiiObfuscator.piiProcess(line, header, records, UNIT_SEPARATOR);
        assertEquals("data" + UNIT_SEPARATOR + "data", returnVal.trim());

        //Tests when working correctly
        String[] line2 = {"data", "data", "nullCol"};
        String[] header2 = {"col1", "col2", null};
        ArrayList<SchemaRecord> records2 = new ArrayList<SchemaRecord>();
        records.add(new SchemaRecord("col1", "test", ""));
        records.add(new SchemaRecord("col2", "test", ""));
        records.add(new SchemaRecord("piiCol", "test", "remove"));
        try {
            String returnVal2 = PiiObfuscator.piiProcess(line2, header2, records2, UNIT_SEPARATOR);
            fail("Should have thrown an exception");
        }
        catch(Exception e){

        }
    }


    @Test
    public void testRemovePii()  throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException{
        //Test if it does contain remove
        ArrayList<SchemaRecord> records = new ArrayList<SchemaRecord>();
        records.add(new SchemaRecord("test", "test", "remove"));

        Object o = PiiObfuscator.hasRemoveComment(records);
        assertEquals("true", o.toString());

        //Test if it doesn't containt remove
        ArrayList<SchemaRecord> records2 = new ArrayList<SchemaRecord>();
        records.add(new SchemaRecord("test", "test", ""));
        records.add(null);
        Object o2 = PiiObfuscator.hasRemoveComment(records2);
        assertEquals("false", o2.toString());

    }

    @Test
    public void testProcessLine() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        ArrayList<String> testingLines = new ArrayList<String>();
        Path p = new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01/Manifest");
        testingLines.add("transaction_4.0_20150812090417_1562.asv\u001F16\u001Fcedba453ff7e04711c2de5a2781749a5c857ac647f09ac17b3d5ac9e7f8e5ac52db197d8e8fe63a4215b9d295abb10e6\u001F08/12/2015 09:08:22\u001F‑");
        testingLines.add("allergy_4.0_20150812090417_3433.asv\u001F0\u001F­cedba453ff7e04711c2de5a2781749a5­c857ac647f09ac17b3d5ac9e7f8e5ac52db197d8e8fe63a4215b9d295abb10e6­08/12/2015 09:08:22-");
        testingLines.add("allergy\u001F111\u001Fhfdhkflds");
        String entity = "";
        Method methodProcessLine = DivisionalDriver.class.getDeclaredMethod("processLine", Path.class, String.class);
        Field entityField = divisionalDriverClass.getDeclaredField("entity");
        entityField.setAccessible(true);
        methodProcessLine.setAccessible(true);
        //tests when data is there
        methodProcessLine.invoke(divisionalDriver, p, testingLines.get(0));
        entity = (String) entityField.get(divisionalDriver);
        assertEquals(entity, "transaction");
        entityField.set(divisionalDriver, "");
        //Tests if no data second index equals zero
        methodProcessLine.invoke(divisionalDriver, p, testingLines.get(1));
        entity = (String) entityField.get(divisionalDriver);
        assertEquals(entity, "");
        //Tests bad data
        try{
            methodProcessLine.invoke(divisionalDriver, p, testingLines.get(2));
            fail("Method should have thrown an exception");
        }
        catch (Exception e){

        }

    }

    @Test
    public void testRegex() throws IOException, NoSuchMethodException, NoSuchFieldException, IllegalAccessException, InvocationTargetException{
        String header = "NUMBER\u001FVARCHAR\u001FNUMBER\u001FNUMBER\u001FNUMBER\u001FVARCHAR\u001FDATETIME\u001FVARCHAR\u001FDATETIME\u001FVARCHAR\u001FVARCHAR\036";
        String line = "1563\u001FGA - CHS Hidden Valley Medical Center\u001F8764\u001F629845\u001F1826756\u001Fmrobinson36\u001F08/08/2015 13:11:02\u001F\u001F\u001F\u001Ffollow up";
        String lineBroken = "1563\u001FGA - CHS Hidden Valley Medical Center\u001F8g764\u001F629845\u001F1826756\u001Fmrobinson36\u001F08/08/2015 13:11:02\u001F\u001F\u001F\u001Ffollow up";

        String header3 = "NUMBER\u001FVARCHAR\u001FDATETIME\u001FNUMBER\036";
        String line3 = "1563.33\u001FF\u001F1-1-1\u001F111";
        String header2 = "NUMBER\u001FVARCHAR\u001FDATETIME";
        String decimalLine = "1563.33\u001FF\u001F1-1-1\036";
        String intLine =  "1563\u001FF\u001F1-1-1";
        String pattern  =  ChsUtils.getPatternMatch(header);
        String pattern2  =  ChsUtils.getPatternMatch(header2);
        String pattern3  =  ChsUtils.getPatternMatch(header3);
        assertEquals(true, Pattern.matches(pattern, line));
        assertEquals(false, Pattern.matches(pattern, lineBroken));
        assertEquals(true, Pattern.matches(pattern2, decimalLine));
        assertEquals(true, Pattern.matches(pattern2, intLine));
        assertEquals(true, Pattern.matches(pattern3, line3));
    }




    @Test
    public void testGetManifestPaths2() throws IOException, NoSuchMethodException, NoSuchFieldException, IllegalAccessException, InvocationTargetException {
        Method method = DivisionalDriver.class.getDeclaredMethod("getManifestPaths", String.class);
        method.setAccessible(true);
        FileSystem fs= mock(FileSystem.class);
        FSDataOutputStream out = mock(FSDataOutputStream.class);
        String testDivisionalWildCard = "/user/financialDataFeed/data/*/athena/finished/2015-09-01";
        FileStatus[] return3 = new FileStatus[2];
        return3[0] = new FileStatus(0, true, 0, 0, 0, new Path("/user/financialDataFeed/data/1113"));
        return3[1] = new FileStatus(0, true, 0, 0, 0, new Path("/user/financialDataFeed/data/3223"));
        FileStatus[] return4 = new FileStatus[2];
       return4[0] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01/Control"));
        return4[1] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01/Manifest"));
        FileStatus[] return5 = new FileStatus[2];
        return4[0] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/3223/athena/finished/2015-09-01/Control"));
        return4[1] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/3223/athena/finished/2015-09-01/Manifest"));
        when(fs.listStatus((Path) anyObject())).thenReturn(return3).thenReturn(return4);
        when(fs.append((Path) anyObject())).thenReturn(out);
        when(fs.exists((Path) anyObject())).thenReturn(true).thenReturn(false);
        Field fileSystem = divisionalDriverClass.getDeclaredField("fs");
        fileSystem.setAccessible(true);
        fileSystem.set(divisionalDriver, fs);
        Field manifestFiles = divisionalDriverClass.getDeclaredField("manifestFiles");
        manifestFiles.setAccessible(true);
        Field controlFiles = divisionalDriverClass.getDeclaredField("controlFiles");
        controlFiles.setAccessible(true);
        Field input = divisionalDriverClass.getDeclaredField("input_path");
        input.setAccessible(true);
        input.set(divisionalDriver, testDivisionalWildCard);
        try {
            method.invoke(divisionalDriver, testDivisionalWildCard);
        }
        catch(Exception e){
            assertEquals(2, ((ArrayList) manifestFiles.get(divisionalDriver)).size());
        }
    }


    @Test
    public void testGetManifestPaths1() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, NoSuchFieldException, IOException {

        //DATE WILD CARD TEST
        String testDateWildCard = "/user/financialDataFeed/data/1111/athena/finished/2015-09*";
        FileSystem fs= mock(FileSystem.class);
        FSDataOutputStream out = mock(FSDataOutputStream.class);
        FileStatus[] return1 = new FileStatus[2];
        return1[0] = new FileStatus(0, true, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01"));
        return1[1] = new FileStatus(0, true, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-02"));
        FileStatus[] return2 = new FileStatus[4];
        return2[0] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01/Manifest"));
        return2[1] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-02/Manifest"));
        return2[2] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01/Control"));
        return2[3] = new FileStatus(0, false, 0, 0, 0, new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-02/Control"));
        when(fs.listStatus((Path) anyObject())).thenReturn(return1).thenReturn(return2);
        when(fs.append((Path) anyObject())).thenReturn(out);
        when(fs.exists((Path) anyObject())).thenReturn(true).thenReturn(true).thenReturn(false);
        Method method = DivisionalDriver.class.getDeclaredMethod("getManifestPaths", String.class);
        method.setAccessible(true);
        Field fileSystem = divisionalDriverClass.getDeclaredField("fs");
        fileSystem.setAccessible(true);
        fileSystem.set(divisionalDriver, fs);
        Field manifestFiles = divisionalDriverClass.getDeclaredField("manifestFiles");
        manifestFiles.setAccessible(true);
        Field controlFiles = divisionalDriverClass.getDeclaredField("controlFiles");
        controlFiles.setAccessible(true);
        Field input = divisionalDriverClass.getDeclaredField("input_path");
        input.setAccessible(true);
        input.set(divisionalDriver, testDateWildCard);
        try {
            method.invoke(divisionalDriver, testDateWildCard);
        }
        catch(Exception e){
            assertEquals(2, ((ArrayList)manifestFiles.get(divisionalDriver)).size());
            assertEquals(2, ((ArrayList)controlFiles.get(divisionalDriver)).size());
            ((ArrayList)manifestFiles.get(divisionalDriver)).clear();
        }

    }

    @Test
    public void testRemoveUnusedControlFiles() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, NoSuchFieldException {

        Set<Path> controlFiles = new LinkedHashSet<Path>();
        controlFiles.add(new Path("/user/financialDataFeed/data/*/athena/finished/2015-09-01"));
        controlFiles.add(new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01"));
        controlFiles.add(new Path("/user/financialDataFeed/data/1122/athena/finished/2015-09-01"));

        Set<Path> manifestFiles = new LinkedHashSet<Path>();
        manifestFiles.add(new Path("1111"));
        manifestFiles.add(new Path("*"));

        Field controlFilesField = divisionalDriverClass.getDeclaredField("controlFiles");
        controlFilesField.setAccessible(true);
        controlFilesField.set(divisionalDriver, controlFiles);

        Field manifestFilesField = divisionalDriverClass.getDeclaredField("manifestFiles");
        manifestFilesField.setAccessible(true);
        manifestFilesField.set(divisionalDriver, manifestFiles);

        Method method = DivisionalDriver.class.getDeclaredMethod("removeUnusedControlFiles");
        method.setAccessible(true);
        method.invoke(divisionalDriver);

        Set<Path> expectedList = new LinkedHashSet<Path>();
        expectedList.add(new Path("/user/financialDataFeed/data/*/athena/finished/2015-09-01"));
        expectedList.add(new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01"));

        assert(controlFiles.size() == 2);
        Assert.assertThat(controlFiles,
                IsIterableContainingInOrder.contains(expectedList.toArray()));

    }

    @Test
    public void testAppendTimeAndExtension() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException {

        String input = "filename";
        Method method = DivisionalDriver.class.getDeclaredMethod("appendTimeAndExtension", String.class);
        method.setAccessible(true);
        String output = (String) method.invoke(divisionalDriver, input);

        assertNotNull(output);

        String pattern = "(filename)(\\.\\d{13})(\\.txt)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(output);
        assertTrue(m.find());
    }


}