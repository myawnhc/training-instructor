package com.hazelcast.inv;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InventoryDB {
    private final static ILogger log = Logger.getLogger(InventoryDB.class);

    private Connection conn;

    private static final String createDatabaseString = "create database inventoryDB";
    private static final String dropDatabaseString   = "drop database if exists inventoryDB";

    protected synchronized void establishConnection()  {
        try {
            // Register the driver, we don't need to actually assign the class to anything
            Class.forName("org.mariadb.jdbc.Driver");
            String jdbcURL = "jdbc:mysql://127.0.0.1:3306/";
            System.out.println("JDBC URL is " + jdbcURL);
            conn = DriverManager.getConnection(
                    jdbcURL, "hzuser", "hzpass");
            log.info("Established connection to MySQL/MariaDB server");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    protected synchronized void createDatabase() {
        if (conn == null) {
            throw new IllegalStateException("Must establish connection before creating the database!");
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(dropDatabaseString);
            log.info("Dropped (if exists) database inventory");
            stmt.executeUpdate(createDatabaseString);
            log.info("Created database inventory");

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    // For testing purposes
    public static void main(String[] args) {
        InventoryDB main = new InventoryDB();
        main.establishConnection();
        main.createDatabase();
    }
}



