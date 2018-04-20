package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.map.impl.record.Records.buildRecordInfo;

/**
 * Replicates all IMap-states of this partition to a replica partition.
 */
public class MapReplicationOperation39 extends MapReplicationOperation {

    // keep these fields `protected`, extended in another context.
    protected final MapReplicationStateHolder39 mapReplicationStateHolder = new MapReplicationStateHolder39(this);
    protected final WriteBehindStateHolder writeBehindStateHolder = new WriteBehindStateHolder(this);
    protected final MapNearCacheStateHolder mapNearCacheStateHolder = new MapNearCacheStateHolder(this);

    public MapReplicationOperation39() {
    }

    public MapReplicationOperation39(PartitionContainer container, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Collection<ServiceNamespace> namespaces = container.getAllNamespaces(replicaIndex);
        this.mapReplicationStateHolder.prepare(container, namespaces, replicaIndex);
        this.writeBehindStateHolder.prepare(container, namespaces, replicaIndex);
        this.mapNearCacheStateHolder.prepare(container, namespaces, replicaIndex);
    }

    public MapReplicationOperation39(PartitionContainer container, Collection<ServiceNamespace> namespaces,
                                   int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.mapReplicationStateHolder.prepare(container, namespaces, replicaIndex);
        this.writeBehindStateHolder.prepare(container, namespaces, replicaIndex);
        this.mapNearCacheStateHolder.prepare(container, namespaces, replicaIndex);
    }

    @Override
    public void run() {
        mapReplicationStateHolder.applyState();
        writeBehindStateHolder.applyState();
        if (getReplicaIndex() == 0) {
            mapNearCacheStateHolder.applyState();
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(final ObjectDataOutput out) throws IOException {
        mapReplicationStateHolder.writeData(out);
        writeBehindStateHolder.writeData(out);
        mapNearCacheStateHolder.writeData(out);
    }

    @Override
    protected void readInternal(final ObjectDataInput in) throws IOException {
        mapReplicationStateHolder.readData(in);
        writeBehindStateHolder.readData(in);
        mapNearCacheStateHolder.readData(in);
    }

    RecordReplicationInfo toReplicationInfo(Record record, SerializationService ss) {
        RecordInfo info = buildRecordInfo(record);
        return new RecordReplicationInfo(record.getKey(), ss.toData(record.getValue()), info);
    }

    RecordStore getRecordStore(String mapName) {
        final boolean skipLoadingOnRecordStoreCreate = true;
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(), mapName, skipLoadingOnRecordStoreCreate);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_REPLICATION;
    }
}