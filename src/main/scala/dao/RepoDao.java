package dao;

import classes.Repo;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class RepoDao {
    private static Properties properties;
    private static FileInputStream in;
    private static String driver;
    private static Connection conn;
    private static String url, username, password;


    public static void connection() {
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
    }

    public static void insertRepo(Repo repo) {

        try {
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO actor (id, name, url) VALUES (" + repo.id() + "','" + repo.name() + "','" + repo.url() + "');";
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void viewRepo() {

        try {
            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM repo";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String url = rs.getString("url");
                System.out.println(id + " " + name + " " + url);
            }
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
