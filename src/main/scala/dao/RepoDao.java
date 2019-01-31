package dao;

import classes.Repo;

import java.sql.*;

public class RepoDao {

    public static void insertRepo(Repo repo) {
        Connection conn = DbHelper.connection();
        try {
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO repo (id, name, url) VALUES (" + repo.id() + "','" + repo.name() + "','" + repo.url() + "');";
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void viewRepo() {
        Connection conn = DbHelper.connection();
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
