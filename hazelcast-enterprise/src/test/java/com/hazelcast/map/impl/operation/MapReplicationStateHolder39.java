package com.hazelcast.map.impl.operation;

import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordReplicationInfo;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.MapIndexInfo;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ThreadUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.impl.record.Records.applyRecordInfo;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Holder for raw IMap key-value pairs and their metadata.
 */
// keep this `protected`, extended in another context.
public class MapReplicationStateHolder39  implements IdentifiedDataSerializable, Versioned {

    // data for each map
    protected Map<String, Set<RecordReplicationInfo>> data;

    // propagates the information if the given record store has been already loaded with map-loaded
    // if so, the loading won't be triggered again after a migration to avoid duplicate loading.
    private Map<String, Boolean> loaded;

    // Definitions of indexes for each map. The indexes are sent in the map-replication operation for each partition
    // since only this approach guarantees that that there is no race between index migration and data migration.
    // Earlier the index definition used to arrive in the post-join operations, but these operation has no guarantee
    // on order of execution, so it was possible that the post-join operations were executed after some map-replication
    // operations, which meant that the index did not include some data.
    private List<MapIndexInfo> mapIndexInfos;

    private MapReplicationOperation operation;

    // holds recordStore-references of this partitions' maps
    private transient Map<String, RecordStore<Record>> storesByMapName;

    /**
     * This constructor exists solely for instantiation by {@code MapDataSerializerHook}. The object is not ready to use
     * unless {@code operation} is set.
     */
    public MapReplicationStateHolder39() {
    }

    public MapReplicationStateHolder39(MapReplicationOperation operation) {
        this.operation = operation;
    }

    void prepare(PartitionContainer container, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        storesByMapName = createHashMap(namespaces.size());

        data = new HashMap<String, Set<RecordReplicationInfo>>(namespaces.size());
        loaded = new HashMap<String, Boolean>(namespaces.size());
        mapIndexInfos = new ArrayList<MapIndexInfo>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace mapNamespace = (ObjectNamespace) namespace;
            String mapName = mapNamespace.getObjectName();
            RecordStore recordStore = container.getRecordStore(mapName);
            if (recordStore == null) {
                continue;
            }

            MapContainer mapContainer = recordStore.getMapContainer();
            MapConfig mapConfig = mapContainer.getMapConfig();
            if (mapConfig.getTotalBackupCount() < replicaIndex) {
                continue;
            }

            loaded.put(mapName, recordStore.isLoaded());
            storesByMapName.put(mapName, recordStore);

            Set<IndexInfo> indexInfos = new HashSet<IndexInfo>();
            if (mapContainer.isGlobalIndexEnabled()) {
                // global-index
                for (Index index : mapContainer.getIndexes().getIndexes()) {
                    indexInfos.add(new IndexInfo(index.getAttributeName(), index.isOrdered()));
                }
            } else {
                // partitioned-index
                final Indexes indexes = mapContainer.getIndexes(container.getPartitionId());
                if (indexes != null && indexes.hasIndex()) {
                    for (Index index : indexes.getIndexes()) {
                        indexInfos.add(new IndexInfo(index.getAttributeName(), index.isOrdered()));
                    }
                }
            }
            MapIndexInfo mapIndexInfo = new MapIndexInfo(mapName);
            mapIndexInfo.addIndexInfos(indexInfos);
            mapIndexInfos.add(mapIndexInfo);
        }
    }

    void applyState() {
        ThreadUtil.assertRunningOnPartitionThread();

        // the null check can be removed in 3.10+ codebase
        if (mapIndexInfos != null) {
            for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
                addIndexes(mapIndexInfo.getMapName(), mapIndexInfo.getIndexInfos());
            }
        }

        // RU_COMPAT_38
        // Old nodes (3.8-) won't send mapIndexInfos to new nodes (3.9+) in the map-replication operation.
        // This is the reason why we pick up the mapContainer.getIndexesToAdd() that were added by the PostJoinMapOperation
        // and we add them to the map, before we add data
        for (String mapName : data.keySet()) {
            RecordStore recordStore = operation.getRecordStore(mapName);
            MapContainer mapContainer = recordStore.getMapContainer();
            addIndexes(mapName, mapContainer.getPartitionIndexesToAdd());
        }

        if (data != null) {
            for (Map.Entry<String, Set<RecordReplicationInfo>> dataEntry : data.entrySet()) {
                Set<RecordReplicationInfo> recordReplicationInfos = dataEntry.getValue();
                final String mapName = dataEntry.getKey();
                RecordStore recordStore = operation.getRecordStore(mapName);
                recordStore.reset();
                recordStore.setPreMigrationLoadedStatus(loaded.get(mapName));

                MapContainer mapContainer = recordStore.getMapContainer();
                PartitionContainer partitionContainer = recordStore.getMapContainer().getMapServiceContext()
                                                                   .getPartitionContainer(operation.getPartitionId());
                for (Map.Entry<String, Boolean> indexDefinition : mapContainer.getIndexDefinitions().entrySet()) {
                    Indexes indexes = mapContainer.getIndexes(partitionContainer.getPartitionId());
                    indexes.addOrGetIndex(indexDefinition.getKey(), indexDefinition.getValue());
                }

                for (RecordReplicationInfo recordReplicationInfo : recordReplicationInfos) {
                    Data key = recordReplicationInfo.getKey();
                    final Data value = recordReplicationInfo.getValue();
                    Record newRecord = recordStore.createRecord(value, DEFAULT_TTL, Clock.currentTimeMillis());
                    applyRecordInfo(newRecord, recordReplicationInfo);
                    recordStore.putRecord(key, newRecord);

                    recordStore.evictEntries(key);
                    recordStore.disposeDeferredBlocks();
                }
            }
        }
    }

    private void addIndexes(String mapName, Collection<IndexInfo> indexInfos) {
        if (indexInfos == null) {
            return;
        }
        RecordStore recordStore = operation.getRecordStore(mapName);
        MapContainer mapContainer = recordStore.getMapContainer();
        if (mapContainer.isGlobalIndexEnabled()) {
            // creating global indexes on partition thread in case they do not exist
            for (IndexInfo indexInfo : indexInfos) {
                Indexes indexes = mapContainer.getIndexes();
                // optimisation not to synchronize each partition thread on the addOrGetIndex method
                if (indexes.getIndex(indexInfo.getAttributeName()) == null) {
                    indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
                }
            }
        } else {
            Indexes indexes = mapContainer.getIndexes(operation.getPartitionId());
            for (IndexInfo indexInfo : indexInfos) {
                indexes.addOrGetIndex(indexInfo.getAttributeName(), indexInfo.isOrdered());
            }
        }
    }

    private static SerializationService getSerializationService(RecordStore recordStore) {
        return recordStore.getMapContainer().getMapServiceContext().getNodeEngine().getSerializationService();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(storesByMapName.size());

        for (Map.Entry<String, RecordStore<Record>> entry : storesByMapName.entrySet()) {
            String mapName = entry.getKey();
            RecordStore recordStore = entry.getValue();

            SerializationService ss = getSerializationService(recordStore);

            out.writeUTF(mapName);
            out.writeInt(recordStore.size());

            Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                Record record = iterator.next();
                RecordReplicationInfo replicationInfo = operation.toReplicationInfo(record, ss);
                out.writeObject(replicationInfo);
            }
        }

        out.writeInt(loaded.size());
        for (Map.Entry<String, Boolean> loadedEntry : loaded.entrySet()) {
            out.writeUTF(loadedEntry.getKey());
            out.writeBoolean(loadedEntry.getValue());
        }

        // RU_COMPAT_39, the check can be removed in 3.9+ (the data should be then send unconditionally)
        // This information is carried over only for 3.9+ cluster versions
        if (out.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            out.writeInt(mapIndexInfos.size());
            for (MapIndexInfo mapIndexInfo : mapIndexInfos) {
                out.writeObject(mapIndexInfo);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        data = new HashMap<String, Set<RecordReplicationInfo>>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            int mapSize = in.readInt();
            Set<RecordReplicationInfo> recordReplicationInfos = new HashSet<RecordReplicationInfo>(mapSize);
            for (int j = 0; j < mapSize; j++) {
                RecordReplicationInfo recordReplicationInfo = in.readObject();
                recordReplicationInfos.add(recordReplicationInfo);
            }
            data.put(name, recordReplicationInfos);
        }

        int loadedSize = in.readInt();
        loaded = new HashMap<String, Boolean>(loadedSize);
        for (int i = 0; i < loadedSize; i++) {
            loaded.put(in.readUTF(), in.readBoolean());
        }

        // RU_COMPAT_39, the check can be removed in 3.9+ (the data should be then read unconditionally)
        // This information is carried over only for 3.9+ cluster versions
        if (in.getVersion().isGreaterOrEqual(Versions.V3_9)) {
            int mapIndexInfosSize = in.readInt();
            mapIndexInfos = new ArrayList<MapIndexInfo>(mapIndexInfosSize);
            for (int i = 0; i < mapIndexInfosSize; i++) {
                MapIndexInfo mapIndexInfo = in.readObject();
                mapIndexInfos.add(mapIndexInfo);
            }
        } else {
            // setting to null means we operate in 3.8- compatibility mode
            mapIndexInfos = null;
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_REPLICATION_STATE_HOLDER;
    }
}