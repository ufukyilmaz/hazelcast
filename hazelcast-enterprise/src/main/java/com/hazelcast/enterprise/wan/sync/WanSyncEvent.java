package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * A marker event to initiate WAN sync
 */
public class WanSyncEvent implements DataSerializable {

    private WanSyncType type;
    private String name;

    public WanSyncEvent() {
    }

    public WanSyncEvent(WanSyncType type, String name) {
        this.type = type;
        this.name = name;
    }

    public WanSyncType getType() {
        return type;
    }

    public void setType(WanSyncType type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(type.getType());
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        type = WanSyncType.getByType(in.readInt());
        name = in.readUTF();
    }
}
