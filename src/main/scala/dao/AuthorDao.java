package dao;

import classes.Actor;
import classes.Author;

import java.sql.*;

public class AuthorDao {

    private static String url = "jdbc:postgresql://localhost:5432/gitArchiveProject";
    private static String user = "postgres";
    private static String password = "root";

    public static void connection() {
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT VERSION()");
            if (resultSet.next()) {
                System.out.println("result: " + resultSet.getString(1));
                conn.close();
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void insertAuthor(Author author) {

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO author (name, email) VALUES (" + author.name() + "','" + author.email() + "');";
            statement.executeUpdate(sql);
            statement.close();
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void viewAuthor() {

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM author";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                String name = rs.getString("name");
                String email = rs.getString("email");

                System.out.println(name + " " + email);
            }
            statement.close();
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
