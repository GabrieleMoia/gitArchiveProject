package dao;

import classes.Author;

import java.sql.*;

public class AuthorDao {

    public static void insertAuthor(Author author) {
        Connection conn = DbHelper.connection();
        try {
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO author (name, email) VALUES ('" + author.name() + "','" + author.email() + "');";
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void viewAuthor() {
        Connection conn = DbHelper.connection();
        try {
            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM author";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                String name = rs.getString("name");
                String email = rs.getString("email");
                System.out.println(name + " " + email);
            }
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
