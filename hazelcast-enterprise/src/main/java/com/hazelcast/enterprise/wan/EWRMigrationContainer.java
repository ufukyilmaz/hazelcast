package com.hazelcast.enterprise.wan;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.MapUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Migration data holder
 */
public class EWRMigrationContainer implements IdentifiedDataSerializable {

    private static final int DEFAULT_DATA_STRUCTURE_COUNT = 10;

    private Map<String, Map<String, PartitionWanEventQueueMap>> migrationContainer =
            MapUtil.createHashMap(DEFAULT_DATA_STRUCTURE_COUNT);

    public void add(String wanRepName, String target, PartitionWanEventQueueMap eventQueueMap) {
        Map<String, PartitionWanEventQueueMap> wanRepContainer = migrationContainer.get(wanRepName);
        if (wanRepContainer == null) {
            wanRepContainer = MapUtil.createHashMap(DEFAULT_DATA_STRUCTURE_COUNT);
            migrationContainer.put(wanRepName, wanRepContainer);
        }
        wanRepContainer.put(target, eventQueueMap);
    }

    public int getMigrationContainerSize() {
        return migrationContainer.size();
    }

    public Map<String, Map<String, PartitionWanEventQueueMap>> getMigrationContainer() {
        return migrationContainer;
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
        for (Map.Entry<String, Map<String, PartitionWanEventQueueMap>>  entry : migrationContainer.entrySet()) {
            out.writeUTF(entry.getKey());
            Map<String, PartitionWanEventQueueMap> partitionWanEventContainerMap = entry.getValue();
            out.writeInt(partitionWanEventContainerMap.size());
            for (Map.Entry<String, PartitionWanEventQueueMap> publisher : partitionWanEventContainerMap.entrySet()) {
                out.writeUTF(publisher.getKey());
                out.writeObject(publisher.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String wanRepName = in.readUTF();
            int publisherEventContainerSize = in.readInt();
            for (int j = 0; j < publisherEventContainerSize; j++) {
                String publisherName = in.readUTF();
                PartitionWanEventQueueMap container = in.readObject();
                add(wanRepName, publisherName, container);
            }
        }
    }
}
