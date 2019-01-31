package dao;

import classes.Actor;

import java.sql.*;

public class ActorDao {

    public static void insertActor(Actor actor) {
        Connection conn = DbHelper.connection();
        try {
            Statement statement = conn.createStatement();
            String sql = "INSERT INTO actor (id, login, display_login, gravatar_id, url, avatar_url) VALUES (" + actor.id() + ",'" + actor.login() + "','" + actor.display_login() + "','" + actor.gravatar_id() + "','" + actor.url() + "','" + actor.avatar_url() + "');";
            statement.executeUpdate(sql);
            statement.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void viewActor() {
        Connection conn = DbHelper.connection();
        try {
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
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}
