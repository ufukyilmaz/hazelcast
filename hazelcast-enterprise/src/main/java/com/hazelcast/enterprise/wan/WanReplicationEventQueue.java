package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Wan replication event queue wrapper
 */
public class WanReplicationEventQueue extends ConcurrentLinkedQueue<WanReplicationEvent> implements DataSerializable {

    private int backupCount;

    public WanReplicationEventQueue() {

    }

    public WanReplicationEventQueue(int backupCount) {
        this.backupCount = backupCount;
    }

    public int getBackupCount() {
        return backupCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(backupCount);
        out.writeInt(size());
        WanReplicationEvent event = poll();
        while (event != null) {
            event.writeData(out);
            event = poll();
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        backupCount = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            WanReplicationEvent event = new WanReplicationEvent();
            event.readData(in);
            if (event.getServiceName() != null) {
                offer(event);
            }
        }
    }
}
