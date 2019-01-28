package dao;

import classes.Actor;

import java.sql.*;

public class ActorDao {

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

    public static void insertActor(Actor actor) {

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO actor (id, login, display_login, gravatar_id, url, avatar_url) VALUES (" + actor.id() + ",'" + actor.login() + "','" + actor.display_login() + "','" + actor.gravatar_id() + "','" + actor.url() + "','" + actor.avatar_url() + "');";
            statement.executeUpdate(sql);
            statement.close();
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void viewActor() {

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, password);
            Statement statement = conn.createStatement();
            String sql = "SELECT * FROM actor";
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                int id = rs.getInt("id");
                String login = rs.getString("login");
                String display_login = rs.getString("display_login");
                String gravatar_id = rs.getString("gravatar_id");
                String url = rs.getString("url");
                String avatar_url = rs.getString("avatar_url");

                System.out.println(id + " " + login + " " + display_login + " " + gravatar_id + " " + url + " " + avatar_url);
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
