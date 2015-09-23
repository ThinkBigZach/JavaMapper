package com.chs.drivers;

import org.apache.hadoop.fs.Path;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void testGetManifestPaths() throws NoSuchMethodException,
        InvocationTargetException, IllegalAccessException {

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