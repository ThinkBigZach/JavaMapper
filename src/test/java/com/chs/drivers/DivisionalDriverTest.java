package com.chs.drivers;

import com.sun.org.apache.xpath.internal.operations.Div;
import org.apache.hadoop.fs.Path;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.*;
import org.junit.rules.ExpectedException;

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
    public void testGetManifestPaths() throws NoSuchMethodException,
            InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        String testDivisionalWildCard = "/user/financialDataFeed/data/*/athena/finished/2015-09-01";
        String testDateWildCard = "/user/financialDataFeed/data/1111/athena/finished/2015-09*";
        

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