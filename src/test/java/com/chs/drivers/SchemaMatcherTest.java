package com.chs.drivers;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.MAP;
import org.junit.Before;
import org.junit.Test;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;

import com.chs.utils.SchemaMatcher;
import com.chs.utils.SchemaRecord;
import com.chs.utils.TDConnector;

@SuppressStaticInitializationFor("com.chs.utils.SchemaMatcher")
@PrepareForTest({SchemaMatcher.class})
public class SchemaMatcherTest {
	
	private static String delimiter = "\036";
	private static String spacelimiter = "\037";
	public String inputentity = "allergy";
	
	//private SchemaMatcher schemaMatcher = new SchemaMatcher();
	//Class<?> schemaMatcherClass = schemaMatcher.getClass();
	SchemaMatcher schematch;
	Map<String, List<SchemaRecord>> tdSchemaListMock;
	
	@Before
	public void setUp() throws Exception 
	{
		schematch = mock(SchemaMatcher.class);
	}

	@Test
	public void testExtractMapFromFile() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		//when(con.getSchemas()).thenReturn(tdSchemaListMock);
		Method methodExtractMapFromFile = SchemaMatcher.class.getDeclaredMethod("extractMapFromFile", String[].class, String[].class);
		methodExtractMapFromFile.setAccessible(true);
		String[] colNames = { "CONTEXT_ID", "CONTEXT_NAME", "Onset Date" };
		String[] colTypes = { "0", "0", "0" };
		Map<String,String> outMap = (Map) methodExtractMapFromFile.invoke(schematch, colNames, colTypes);
		assertNotNull(outMap);
	}
	
	@Test
	public void testExtractMapFromFile_ForExtra() throws NoSuchMethodException, SecurityException
	{
		Method methodExtractMapFromFile = SchemaMatcher.class.getDeclaredMethod("extractMapFromFile", String[].class, String[].class);
		methodExtractMapFromFile.setAccessible(true);
		String[] colNames = { "CONTEXT_ID", "CONTEXT_NAME", "Onset Date", "Allergy ID"};
		String[] colTypes = { "0", "0", "0" };
		Map<String,String> output = null;
		try
		{
			output = (Map) methodExtractMapFromFile.invoke(schematch, colNames, colTypes);
		} catch (Exception e)
		{
			//do nothing
		}
		finally
		{
			assertNull(output);
		}
	}
	
	@Test
	public void testExtractMapFromFile_ForFewer() throws NoSuchMethodException, SecurityException
	{
		Method methodExtractMapFromFile = SchemaMatcher.class.getDeclaredMethod("extractMapFromFile", String[].class, String[].class);
		methodExtractMapFromFile.setAccessible(true);
		String[] colNames = { "CONTEXT_ID", "CONTEXT_NAME"};
		String[] colTypes = { "0", "0", "0" };
		Map<String,String> output = null;
		try
		{
			output = (Map) methodExtractMapFromFile.invoke(schematch, colNames, colTypes);
		} catch (Exception e)
		{
			//do nothing
		}
		finally
		{
			assertNotNull(output);
		}
	}
		
	@Test
	public void testSchemaMatch() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
//		when(con.getSchemas()).thenReturn(tdSchemaListMock);
		Method methodSchemaMatch = SchemaMatcher.class.getDeclaredMethod("schemaMatch", Map.class, Map.class, int.class, String.class);
		methodSchemaMatch.setAccessible(true);
		Map<String,String> testGoldenMap = new HashMap<String,String>();
		testGoldenMap.put("context_id", "0");
		testGoldenMap.put("context_name", "0");
		testGoldenMap.put("onset_date", "0");
		Map<String,String> testCompareMap = new HashMap<String,String>();
		testCompareMap.put("CONTEXT_ID", "0");
		testCompareMap.put("CONTEXT_NAME", "0");
		testCompareMap.put("Onset Date", "0");
		boolean testout = (Boolean)methodSchemaMatch.invoke(schematch, testGoldenMap, testCompareMap, testGoldenMap.size(), inputentity);
		assertTrue(testout);
	}
	
	@Test
	public void testSchemaMatch_ForExtra() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException 
	{
		Method methodSchemaMatch = SchemaMatcher.class.getDeclaredMethod("schemaMatch", Map.class, Map.class, int.class, String.class);
		methodSchemaMatch.setAccessible(true);
		Map<String,String> testGoldenMap = new HashMap<String,String>();
		testGoldenMap.put("context_id", "0");
		testGoldenMap.put("context_name", "0");
		testGoldenMap.put("onset_date", "0");
		testGoldenMap.put("allergy_id", "0");
		Map<String,String> testCompareMap = new HashMap<String,String>();
		testCompareMap.put("CONTEXT_ID", "0");
		testCompareMap.put("CONTEXT_NAME", "0");
		testCompareMap.put("Onset Date", "0");
		boolean testout = (Boolean)methodSchemaMatch.invoke(schematch, testGoldenMap, testCompareMap, testCompareMap.size(), inputentity);
		assertTrue(testout);
	}
	
	@Test
	public void testSchemaMatch_ForFewer() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		Method methodSchemaMatch = SchemaMatcher.class.getDeclaredMethod("schemaMatch", Map.class, Map.class, int.class, String.class);
		methodSchemaMatch.setAccessible(true);
		Map<String,String> testGoldenMap = new HashMap<String,String>();
		testGoldenMap.put("context_id", "0");
		testGoldenMap.put("onset_date", "0");
		Map<String,String> testCompareMap = new HashMap<String,String>();
		testCompareMap.put("CONTEXT_ID", "0");
		testCompareMap.put("CONTEXT_NAME", "0");
		testCompareMap.put("Onset Date", "0");
		boolean testout = (Boolean)methodSchemaMatch.invoke(schematch, testGoldenMap, testCompareMap, testCompareMap.size(), inputentity);
		assertFalse(testout);
	}
	
	@Test
	public void testCleanStringByColumn() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		Method methodCleanString = SchemaMatcher.class.getDeclaredMethod("cleanStringByColumn", String.class);
		methodCleanString.setAccessible(true);
		String needsCleaning = "CONTEXT_ID" + spacelimiter + "CONTEXT_NAME" + spacelimiter + "CONTEXT_PARENTCONTEXTID" + spacelimiter + 
				"Allergy ID" + spacelimiter + "Type" + spacelimiter + "Patient ID" + spacelimiter + "Chart ID" + spacelimiter + "Allergy Name" + spacelimiter + "Allergy Code";
		String[] cleaned = (String[])methodCleanString.invoke(schematch, needsCleaning);
		assertTrue(cleaned.length >= 8);
	}
}
