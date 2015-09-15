package utils;



import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.hadoop.hive.jdbc.HiveDriver;
/**
 * Created by zw186016 on 9/14/15.
 */
public class HiveConnector {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    private static String CREATE_MANIFEST_TABLE = "CREATE EXTERNAL TABLE manifest (file_name STRING, records INT, hash STRING)" + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\037'";
    public static void executeStatement(String sql) throws SQLException {
        System.out.println("Running: " + sql);
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        Connection con = DriverManager.getConnection("jdbc:hive://nameservice1:10000/default", "", "");
        Statement stmt = con.createStatement();
        stmt.executeQuery(sql);
    }

    public static void loadManifestTable(String fileLocation) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        String loadData = "LOAD DATA LOCAL INPATH '" + fileLocation + "' INTO TABLE manifest";
        Connection con = DriverManager.getConnection("jdbc:hive://nameserver1:10000/default", "", "");
        Statement stmt = con.createStatement();
        stmt.execute("DROP IF EXISTS manifest");
        stmt.execute(CREATE_MANIFEST_TABLE);
        stmt.executeQuery(loadData);
    }
}