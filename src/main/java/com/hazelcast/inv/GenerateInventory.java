package com.hazelcast.inv;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.CompletableFuture;


public class GenerateInventory {

    private final static ILogger log = Logger.getLogger(GenerateInventory.class);

    public static void main(String[] args) {
        //new EnvironmentSetup();
        InventoryDB database = new InventoryDB();
        database.establishConnection(); // connects to server, in a non-db-specific way
        // Database now pre-exists in the Docker image
        //database.createDatabase();

        /////////////// Merchants
        InventoryTable inventoryTable = new InventoryTable();
        inventoryTable.establishConnection();
        // Dstabase and table now pre-exist in Docker image
        //inventoryTable.createInventoryTable();

        log.info("Generating inventory");
        CompletableFuture<Void> inventoryFuture = CompletableFuture.runAsync(() -> {
            inventoryTable.generateSampleData();
            log.info("Generated inventory sample data");
        });


        log.info("All launched, waiting on completion");
        CompletableFuture<Void> all = CompletableFuture.allOf(inventoryFuture);
        try {
            all.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.info("All complete.");
        System.exit(0);
    }
}
