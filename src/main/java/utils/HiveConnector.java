package utils;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;

/**
 * Created by zw186016 on 9/14/15.
 */
public class HiveConnector {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String CREATE_MANIFEST_TABLE = "CREATE EXTERNAL TABLE manifest (file_name STRING, records INT, hash STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\037'";
    private static String CREATE_CONTROL_TABLE = "CREATE EXTERNAL TABLE control (job_id STRING, date STRING, date2 STRING, path STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'";


    private static String CREATE_ENTITY_START = "CREATE EXTERNAL TABLE IF NOT EXISTS ";
    private static String CREATE_ENTITY_END = " (line STRING)";

    public static void executeStatement(String sql) throws SQLException {
        System.out.println("Running: " + sql);
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/a_lynn_db", "", "");
        Statement stmt = con.createStatement();
        stmt.executeQuery(sql);
    }

    //Type is either Manifest, Control
    public static void loadTable(String type, String fileLocation) throws SQLException {

        System.out.println("LOADING TABLE FOR " + type);
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        String loadData = "LOAD DATA LOCAL INPATH '" + fileLocation + "' INTO TABLE " + type;
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "rscott22", "");
        Statement stmt = con.createStatement();
        if (type.equalsIgnoreCase("MANIFEST")) {
            stmt.execute("DROP TABLE IF EXISTS " + type);
            stmt.execute(CREATE_MANIFEST_TABLE);
        } else if (type.equalsIgnoreCase("CONTROL")) {
            stmt.execute("DROP TABLE IF EXISTS " + type);
            stmt.execute(CREATE_CONTROL_TABLE);
        }
        stmt.executeQuery(loadData);
    }


    //Type is Entity
    public static void loadTable(String type, String entity, String fileLocation) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        String loadData = "LOAD DATA LOCAL INPATH '" + fileLocation + "' INTO TABLE " + entity;
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "rscott22", "");
        Statement stmt = con.createStatement();
        String create = CREATE_ENTITY_START + entity + CREATE_ENTITY_END;
        stmt.execute(create);
        stmt.executeQuery(loadData);
    }

    //    If entityFilter is "" then it'll return all entities, otherwise it'll filter to whatever entityFilter is called
    public static ArrayList<String> getManifestLocations(String entityFilter) throws SQLException {
        ArrayList<String> returnList = new ArrayList<String>();
        boolean filter = entityFilter.equals("");
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
        Statement stmt = con.createStatement();
        ResultSet set = stmt.executeQuery("select split(file_name,'_')[0] entity, file_name from manifest where record_count > 0 order by split(file_name,'_')[0];");
        while (!set.isAfterLast()) {
            String path = set.getString(1);
            String entityMatch = set.getString(0);
            if (!filter) {
                if (entityFilter.equalsIgnoreCase(entityMatch)) {
                    returnList.add(path);
                }
            } else {
                returnList.add(path);
            }
            set.next();
        }
        return returnList;
    }


}
