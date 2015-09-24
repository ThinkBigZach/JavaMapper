package com.chs.drivers;
import static org.mockito.Mockito.*;
import com.sun.org.apache.xpath.internal.operations.Div;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class DivisionalDriverTest {


    private String[] args = {"/user/financialDataFeed/data/*/athena/finished/2015-09-01",
            "allergy",
            "/user/athena/data/financialdatafeed/finished/",
            "/enterprise/mappings/athena/chs-practice-id-mapping-athena.csv",
            "/enterprise/mappings/athena/athena_table_defs.csv",
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

        List<Path> controlFiles = new ArrayList<Path>();
        controlFiles.add(new Path("/user/financialDataFeed/data/*/athena/finished/2015-09-01"));
        controlFiles.add(new Path("/user/financialDataFeed/data/1111/athena/finished/2015-09-01"));
        controlFiles.add(new Path("/user/financialDataFeed/data/1122/athena/finished/2015-09-01"));

        List<Path> manifestFiles = new ArrayList<Path>();
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

        List<Path> expectedList = new ArrayList<Path>();
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

    public void testStart() throws Exception {

    }


    public void testRemoveUnusedControlFiles1() throws Exception {

    }
}