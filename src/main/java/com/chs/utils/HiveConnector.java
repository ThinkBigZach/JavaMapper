package com.chs.utils;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zw186016 on 9/14/15.
 */
public class HiveConnector {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String CREATE_MANIFEST_TABLE = "CREATE EXTERNAL TABLE manifest (file_name STRING, records INT, hash STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\037'";
    private static String CREATE_CONTROL_TABLE = "CREATE EXTERNAL TABLE control (job_id STRING, date STRING, date2 STRING, path STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'";


    private static String CREATE_ENTITY_START = "CREATE TABLE IF NOT EXISTS ";
    private static String CREATE_ENTITY_END = " (line STRING)";


    //TODO:  Zach, don't create a new connection each time you need to do a Hive operations.
    //TODO:  create a singleton type of class that does a lazy initialzation.
    //TODO:  Also check and see if the connection needs to be closed upon process termination.

    private static Connection conn;
    public static Connection getConnection() throws SQLException {
        if(conn == null || conn.isClosed()){
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
            conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/testing", "", "");
            return conn;
        }
        else{
            return conn;
        }
    }

    public static void executeStatement(String sql) throws SQLException {
        System.out.println("Running: " + sql);
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeQuery(sql);
    }

    //Type is either Manifest, Control
    public static void loadTable(String type, String fileLocation) throws SQLException {
        System.out.println("LOADING TABLE FOR " + type);
        System.out.println("IS CONNECTION CLOSED?" + conn.isClosed());
        String loadData = "LOAD DATA INPATH '" + fileLocation + "' INTO TABLE " + type;
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        if (type.equalsIgnoreCase("MANIFEST")) {
            stmt.execute("DROP TABLE IF EXISTS " + type);
            stmt.execute(CREATE_MANIFEST_TABLE);
        } else if (type.equalsIgnoreCase("CONTROL")) {
            stmt.execute("DROP TABLE IF EXISTS " + type);
            stmt.execute(CREATE_CONTROL_TABLE);
        }
        stmt.execute(loadData);
    }


    public static void createEntityTables(String entity, String outPath) throws SQLException{
        System.out.println("CREATING ENTITY TABLE " + entity);
        System.out.println("IS CONNECTION CLOSED?" + conn.isClosed());
        String dropTable = "DROP TABLE IF EXISTS " + entity;
        String create = CREATE_ENTITY_START + entity + CREATE_ENTITY_END;
        String data = "LOAD DATA INPATH '" + outPath + entity + ".txt" + "' INTO TABLE "  + entity;
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.execute(dropTable);
        stmt.execute(create);
        stmt.execute(data);
    }


    //Type is Entity
    public static void loadTable(String type, String entity, String fileLocation) throws SQLException {
        String loadData = "LOAD DATA LOCAL INPATH '" + fileLocation + "' INTO TABLE " + entity;
        System.out.println(loadData);
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        String create = CREATE_ENTITY_START + entity + CREATE_ENTITY_END;
        stmt.execute("DROP TABLE IF EXISTS " + entity);
        stmt.execute(create);
        stmt.execute(loadData);
    }

    //    If entityFilter is "" then it'll return all entities, otherwise it'll filter to whatever entityFilter is called
    public static Map<String, ArrayList<String>> getManifestLocations(String entityFilter) throws SQLException {
        HashMap<String, ArrayList<String>> returnList = new HashMap<String, ArrayList<String>>();
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        ResultSet set = stmt.executeQuery("select split(file_name,'_')[0] entity, file_name from manifest where record_count > 0 order by split(file_name,'_')[0];");
        while (!set.isAfterLast()) {
            String path = set.getString(1);
            String entityMatch = set.getString(0);
            if (entityFilter.equalsIgnoreCase(entityMatch) || entityFilter.equals("")) {
                if(returnList.containsKey(entityMatch)){
                    returnList.get(entityMatch).add(path);
                }
                else{
                    ArrayList<String> temp = new ArrayList<String>();
                    temp.add(path);
                    returnList.put(entityMatch, temp);
                }
            }
            set.next();
        }
        return returnList;
    }

}
