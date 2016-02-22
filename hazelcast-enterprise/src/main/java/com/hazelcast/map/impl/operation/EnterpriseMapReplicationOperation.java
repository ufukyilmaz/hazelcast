package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;

import java.util.Map;
import java.util.Set;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 *
 * Additionally deals with HD-IMap specific cases by handling {@link NativeOutOfMemoryError}
 */
public class EnterpriseMapReplicationOperation extends MapReplicationOperation {

    private transient NativeOutOfMemoryError oome;

    public EnterpriseMapReplicationOperation() {
    }

    public EnterpriseMapReplicationOperation(PartitionContainer container, int partitionId, int replicaIndex) {
        super(container, partitionId, replicaIndex);
    }

    @Override
    public void run() {
        try {
            super.run();
        } catch (Throwable e) {
            disposePartition();

            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        disposePartition();

        if (oome != null) {
            getLogger().warning(oome.getMessage());
        }

    }

    private void disposePartition() {
        Map<String, Set<RecordReplicationInfo>> data = mapReplicationStateHolder.data;
        for (String mapName : data.keySet()) {
            dispose(mapName);
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposePartition();
        super.onExecutionFailure(e);
    }

    private void dispose(String mapName) {
        int partitionId = getPartitionId();
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);
        if (recordStore != null) {
            recordStore.disposeDeferredBlocks();
        }
    }

}

