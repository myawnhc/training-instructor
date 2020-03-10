package com.hazelcast.inv;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class InventoryKey implements IdentifiedDataSerializable {
    private String SKU;
    private String location;

    public InventoryKey(String sku, String location) {
        this.SKU = sku;
        this.location = location;
    }

    // no-arg constructor required by IDSFactory
    public InventoryKey() {}

    public String sku() { return SKU; }
    public String location() { return location; }

    @Override
    public String toString() {
        return SKU + location;
    }

    @Override
    public int getFactoryId() {
        return IDSFactory.FACTORY_ID;
    }

    @Override
    public int getId() {
        return IDSFactory.IDS_INVENTORY_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeUTF(SKU);
        objectDataOutput.writeUTF(location);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        SKU = objectDataInput.readUTF();
        location = objectDataInput.readUTF();
    }
}
