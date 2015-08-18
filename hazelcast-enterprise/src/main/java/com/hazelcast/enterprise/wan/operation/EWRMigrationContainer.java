package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.enterprise.wan.WanReplicationEventQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.MapUtil;
import com.hazelcast.wan.WanReplicationEvent;

import java.io.IOException;
import java.util.Map;

/**
 * Migration data holder
 */
public class EWRMigrationContainer implements IdentifiedDataSerializable {

    private static final int DEFAULT_DATA_STRUCTURE_COUNT = 10;

    Map<String, Map<String, Map<String, WanReplicationEventQueue>>> migrationContainer =
            MapUtil.createHashMap(DEFAULT_DATA_STRUCTURE_COUNT);

    public void add(String wanRepName, String target, String name, WanReplicationEventQueue eventQueue) {
        Map<String, Map<String, WanReplicationEventQueue>> wanRepContainer = migrationContainer.get(wanRepName);
        if (wanRepContainer == null) {
            wanRepContainer = MapUtil.createHashMap(DEFAULT_DATA_STRUCTURE_COUNT);
            migrationContainer.put(wanRepName, wanRepContainer);
        }

        Map<String, WanReplicationEventQueue> targetContainer = wanRepContainer.get(target);
        if (targetContainer == null) {
            targetContainer = MapUtil.createHashMap(DEFAULT_DATA_STRUCTURE_COUNT);
            wanRepContainer.put(target, targetContainer);
        }

        targetContainer.put(name, eventQueue);

    }

    public int getMigrationContainerSize() {
        return migrationContainer.size();
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.EWR_QUEUE_CONTAINER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationContainer.size());
        for (Map.Entry<String, Map<String, Map<String, WanReplicationEventQueue>>> mapEntry : migrationContainer.entrySet()) {
            out.writeUTF(mapEntry.getKey());
            Map<String, Map<String, WanReplicationEventQueue>> targetContainer = mapEntry.getValue();
            out.writeInt(targetContainer.size());
            for (Map.Entry<String, Map<String, WanReplicationEventQueue>> targetEntry : targetContainer.entrySet()) {
                out.writeUTF(targetEntry.getKey());
                Map<String, WanReplicationEventQueue> queues = targetEntry.getValue();
                out.writeInt(queues.size());
                for (Map.Entry<String, WanReplicationEventQueue> queueEntry : queues.entrySet()) {
                    out.writeUTF(queueEntry.getKey());
                    WanReplicationEventQueue queue = queueEntry.getValue();
                    out.writeInt(queue.size());
                    for (int i = 0; i < queue.size(); i++) {
                        out.writeObject(queue.poll());
                    }
                }
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int migrationContainerSize = in.readInt();
        for (int i = 0; i < migrationContainerSize; i++) {
            String wanRepName = in.readUTF();
            int targetContainerSize = in.readInt();
            for (int j = 0; j < targetContainerSize; j++) {
                String targetName = in.readUTF();
                int dataStructureCount = in.readInt();
                for (int k = 0; k < dataStructureCount; k++) {
                    String name = in.readUTF();
                    int queueSize = in.readInt();
                    WanReplicationEventQueue eventQueue = new WanReplicationEventQueue();
                    for (int l = 0; l < queueSize; l++) {
                        eventQueue.offer(in.<WanReplicationEvent>readObject());
                    }
                    add(wanRepName, targetName, name, eventQueue);
                }
            }
        }

    }
}
