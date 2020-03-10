package com.hazelcast.inv;

import com.hazelcast.core.MapLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.*;

public class InventoryTable
        implements MapLoader<InventoryKey, Inventory> {

    private Connection conn;

    private final static ILogger log = Logger.getLogger(InventoryTable.class);

    private static final DecimalFormat skuFormat = new DecimalFormat("000000"); // 6 digit
    private static final DecimalFormat locationFormat  = new DecimalFormat( "0000");    // 4 digit

//    protected String accountNumber;
//    private Double creditLimit;
//    private Double balance;
//    private Account.AccountStatus status;
//    //private Location lastReportedLocation;
//    private String lastReportedLocation;


    // Index positions
    private static final int SKU = 1;
    private static final int DESCRIPTION = 2;
    private static final int LOCATION = 3;
    private static final int LOCATION_TYPE = 4;
    private static final int QUANTITY = 5;

//    private static final String createTableString =
//            "create table account ( " +
//                    "acct_number    char(10)     not null, " +
//                    "credit_limit   float, " +
//                    "balance        float, " +
//                    "acct_status    smallint, " +
//                    "location       varchar(10), " +         // geohash
//                    "primary key (acct_number) " +
//                    ")";

//    create table inventory (
//            sku           char(10)     not null,
//    description   char(30),
//    location      char(6),
//    loc_type      char(1),
//    qty           integer,
//    primary key (sku, location)
//);

    private static final String insertTemplate =
            "insert into inventory (sku, description, location, loc_type, qty) " +
                    " values (?, ?, ?, ?, ?)";

    private static final String selectTemplate =
            "select sku, description, location, loc_type, qty from inventory where sku = ? and location = ?";

    //    private static final String selectSKUsString = "select distinct sku from inventory";
//    private static final String selectLocationsString = "select distinct location from inventory";
//
//    private PreparedStatement createStatement;
    private PreparedStatement insertStatement;
    private PreparedStatement selectStatement;

    public synchronized void establishConnection()  {
        //log.info("AbstractTable.establishConnection()");
        try {
            // Register the driver, we don't need to actually assign the class to anything
            Class.forName("org.mariadb.jdbc.Driver");
            String jdbcURL = "jdbc:mysql://127.0.0.1:3306/inventoryDB";
            //log.info("Attempting connection to " + jdbcURL + " for user " + BankInABoxProperties.JDBC_USER);
            conn = DriverManager.getConnection(
                    jdbcURL, "hzuser", "hzpass");
            log.info("Established connection to inventoryDB database");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

//    private Inventory generate(int id) {
//        try {
//            Inventory inv = new Account(accountFormat.format(id));
//            return inv;
//        } catch (Throwable t) {
//            t.printStackTrace();
//            System.exit(-1);
//
//            return null;
//        }
//    }

    public void generateSampleData() {
        // Hard-coding some varlues here:
        // - There are 5 warehouse locations, numbered 1-5
        // - There are 50 store locations, number 101-150.
        // - Each location will get 1000 stock entries

        // Generate warehouse stock
        for (int i=1; i<=5; i++) {
            for (int j=0; j<1000; j++) {
                String SKU = "Item" + skuFormat.format(j);
                String location = locationFormat.format(i);
                Inventory item = new Inventory(SKU, 'W', location);
                writeToDatabase(item);
            }
        }

        // Generate store stock
        for (int i=101; i<=150; i++) {
            for (int j=0; j<1000; j++) {
                String SKU = "Item" + skuFormat.format(j);
                String location = locationFormat.format(i);
                Inventory item = new Inventory(SKU, 'S', location);
                writeToDatabase(item);
            }
        }
    }

//    public synchronized void createInventoryTable()  {
//        try {
//            createStatement = conn.prepareStatement(createTableString);
//            createStatement.executeUpdate();
//            createStatement.close();
//            log.info("Created Inventory table ");
//        } catch (SQLException se) {
//            se.printStackTrace();
//            System.exit(-1);
//        }
//    }

    public synchronized void writeToDatabase(Inventory item) {
        try {
            if (insertStatement == null) {
                insertStatement = conn.prepareStatement(insertTemplate);
            }
            insertStatement.setString(SKU, item.getSKU());
            insertStatement.setString(DESCRIPTION, item.getDescription());
            insertStatement.setString(LOCATION, item.getLocation());
            insertStatement.setString(LOCATION_TYPE, ""+item.getLocationType());
            insertStatement.setInt(QUANTITY, item.getQuantity());
            insertStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public synchronized Inventory readFromDatabase(String skuKey, String location) {
        if (skuKey == null || location == null) {
            log.warning("InventoryTable.readFromDatabase(): Passed null id, returning null");
            return null;
        }
        try {
            if (selectStatement == null) {
                selectStatement = conn.prepareStatement(selectTemplate);
            }
            selectStatement.setString(1, skuKey);
            selectStatement.setString(2, location);
            //log.info("readFromDatabase: " + selectStatement.toString());
            ResultSet rs = selectStatement.executeQuery();
            Inventory item = new Inventory();
            if (rs == null) {
                log.warning("InventoryTable.readFromDatabase(): Null resultSet trying to read SKU " + skuKey + " Locn " + location);
                return null;
            }
            while (rs.next()) {
                item.setSKU(rs.getString(SKU));
                item.setDescription(rs.getString(DESCRIPTION));
                item.setLocation(rs.getString(LOCATION));
                item.setLocationType(rs.getString(LOCATION_TYPE).charAt(0));
                item.setQuantity(rs.getInt(QUANTITY));
            }
            return item;
        } catch (SQLException e) {
            log.info("Error in " + selectStatement.toString() + " --> " + e.getMessage());
            //e.printStackTrace();
            //System.exit(-1);
            return null;
        }
    }

    // MapLoader interface

    @Override
    public synchronized Inventory load(InventoryKey key) {
        if (conn == null)
            establishConnection();
        return readFromDatabase(key.sku(), key.location());
    }

    @Override
    public synchronized Map<InventoryKey, Inventory> loadAll(Collection<InventoryKey> collection) {
        //log.info("InventoryTable.loadAll() with " + collection.size() + " keys");
        if (conn == null)
            establishConnection();
        Map<InventoryKey,Inventory> results = new HashMap<>(collection.size());
        // NOTE: parallelStream here leads to SQLException in read database, so drop back here until we
        // can make that threadsafe. (Trying to use shared PreparedStatement with different parameters)
        collection.stream().forEach((InventoryKey key) -> {
            Inventory item = load(key);
            results.put(key, item);
        });
        return results;
    }

    @Override
    public synchronized Iterable<InventoryKey> loadAllKeys() {
        //log.info("loadAllKeys() on inventoryTable");
        if (conn == null)
            establishConnection();

        // Note this must be kept in sync with GenerateSampleData method
        int size = 100000; // Actually current scheme will produce 55K items
        List<InventoryKey> allKeys = new ArrayList<>(size);

        // Generate warehouse stock
        for (int i=1; i<=5; i++) {
            for (int j=0; j<1000; j++) {
                String SKU = "Item" + skuFormat.format(j);
                String location = locationFormat.format(i);
                InventoryKey key = new InventoryKey(SKU, location);
                allKeys.add(key);
            }
        }

        // Generate store stock
        for (int i=101; i<=150; i++) {
            for (int j=0; j<1000; j++) {
                String SKU = "Item" + skuFormat.format(j);
                String location = locationFormat.format(i);
                InventoryKey key = new InventoryKey(SKU, location);
                allKeys.add(key);
            }
        }

        log.info("MapLoader.loadAllKeys() on inventory table returning " + allKeys.size() + " keys");
        return allKeys;
    }
}
