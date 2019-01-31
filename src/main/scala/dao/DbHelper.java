package dao;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class DbHelper {

    private static Properties properties;
    private static FileInputStream in;
    private static String driver;
    private static Connection conn;
    private static String url, username, password;

    public static Connection connection() {
        try {
            properties = new Properties();
            in = new FileInputStream("db_properties.properties");
            properties.load(in);
            driver = properties.getProperty("jdbc.driver");

            if (driver != null) {
                Class.forName(driver);
                url = properties.getProperty("jdbc.url");
                username = properties.getProperty("jdbc.username");
                password = properties.getProperty("jdbc.password");
                conn = DriverManager.getConnection(url, username, password);
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT VERSION()");
                if (resultSet.next()) {
                    System.out.println("result: " + resultSet.getString(1));
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void close_connecction() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
