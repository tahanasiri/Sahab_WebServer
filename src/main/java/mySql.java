import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class mySql {
    static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/";
    static final String USER = "root";
    static final String PASS = "8121@TahA";
    public static void createDatabase(){
        try{
            Class.forName(DB_DRIVER);
            Connection conn = DriverManager.getConnection(DB_URL,USER,PASS);
            System.out.println("Connection for 'Creating Database' is established successfully:");

            Statement statement = conn.createStatement();

            String createDatabase = "CREATE DATABASE SAHAB_DB";
            statement.executeUpdate(createDatabase);
            System.out.println("Database created successfully...");

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Hello");
        }
    }
    public static void createTable(){
        try{
            Class.forName(DB_DRIVER);
            Connection conn = DriverManager.getConnection((DB_URL+"SAHAB_DB"),USER,PASS);
            System.out.println("Connection for 'Creating Table' is established successfully:");

            Statement statement = conn.createStatement();
            String createTable = "CREATE TABLE MarketInfo (" +
                    "Number Int AUTO_INCREMENT PRIMARY KEY ," +
                    "Rule VARCHAR (255)," +
                    "Market VARCHAR (255)," +
                    "Price VARCHAR (255)," +
                    "CurrentDate DATE," +
                    "CurrentTime TIME)";
            statement.executeUpdate(createTable);
            System.out.println("Table created in given Database...");
            conn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void insertToTable(String rule, String market, String price){
        try{
            Class.forName(DB_DRIVER);
            Connection conn = DriverManager.getConnection((DB_URL+"SAHAB_DB"),USER,PASS);
            System.out.println("Connection for 'Inserting' is established successfully:");

            String sql = "INSERT INTO MarketInfo(Rule, Market, Price, CurrentDate, CurrentTime)" +
                    "VALUES (?, ?, ?, CURDATE(), CURTIME())";
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1, rule);
            preparedStatement.setString(2, market);
            preparedStatement.setString(3, price);
            preparedStatement.executeUpdate();

            System.out.println("Inserted records into the table...");
            conn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
