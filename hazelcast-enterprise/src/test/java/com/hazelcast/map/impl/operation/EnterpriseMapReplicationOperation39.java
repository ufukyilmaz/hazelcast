package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.ServiceNamespace;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 *
 * Additionally deals with Hi-Density backed IMap specific cases by handling {@link NativeOutOfMemoryError}.
 */
public class EnterpriseMapReplicationOperation39
        extends MapReplicationOperation39 {

    private transient NativeOutOfMemoryError oome;

    public EnterpriseMapReplicationOperation39() {
    }

    public EnterpriseMapReplicationOperation39(PartitionContainer container, int partitionId, int replicaIndex) {
        super(container, partitionId, replicaIndex);
    }

    public EnterpriseMapReplicationOperation39(PartitionContainer container, Collection<ServiceNamespace> namespaces,
                                               int partitionId, int replicaIndex) {
        super(container, namespaces, partitionId, replicaIndex);
    }

    @Override
    public void run() {
        try {
            super.run();
        } catch (Throwable e) {
            getLogger().severe("map replication operation failed for partitionId=" + getPartitionId(), e);

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

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MAP_REPLICATION;
    }

}

