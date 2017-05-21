package kafka;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by YJ02 on 5/1/2017.
 */
public  class MySqlExecutor {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  //  static final String DB_URL = "jdbc:mysql://localhost:3600/hive";
  //  static final String USER = "username";
  //  static final String PASS = "password";


    public static void insert2MySQL(String DB_URL,String USER, String PASS, String sql ) {

        try {
            //STEP 2: Register JDBC driver
            Connection conn = null;
            Statement stmt = null;

            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Inserting records into the table...");
            stmt = conn.createStatement();

            stmt.execute(sql);
            System.out.println("Inserted records into the table...");

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static ArrayList<String> getPartitionOffsets(String DB_URL,String USER, String PASS,String table,String topic){
        try {
            //STEP 2: Register JDBC driver
            Connection conn = null;
            Statement stmt = null;

            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query

            String sql="select topic, partition, max(untilloffset) from "+table+" where topic=\""+topic+"\" group by topic, partition";
            System.out.println("SQL="+sql);
            PreparedStatement Statement = conn.prepareStatement(sql);
            ResultSet rs = Statement.executeQuery();
            rs.getFetchSize();
            ArrayList<String> offsets=new ArrayList<String>();
            while(rs.next()){
                System.out.println("1="+rs.getString(1)+"2="+rs.getString(2)+"3="+rs.getString(3));
                offsets.add(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3));
            }
            System.out.println("Returning data from MySQL");
            return offsets;


        } catch (Exception e) {
            System.out.println("Got SQL Exception");
            System.out.println(e.getMessage()); return null;
        }
    }
}
