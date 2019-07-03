package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.hotrestart.HotRestartIntegrationService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Operation used to rebuild Merkle trees on top of a map.
 * Invoked from {@link HotRestartIntegrationService#start()}
 * in local only, therefore no serialization support.
 */
public class MerkleTreeRebuildOperation extends MapOperation implements AllowedDuringPassiveState {

    public MerkleTreeRebuildOperation() {
        super();
    }

    public MerkleTreeRebuildOperation(String mapName) {
        super(mapName);
    }

    @Override
    protected void runInternal() {
        assertRunningOnPartitionThread();

        final int partitionId = getPartitionId();
        final String mapName = getName();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, mapName);
        final MerkleTree merkleTree = getMerkleTree(partitionId, mapName);
        final Iterator<Record> iterator = recordStore.iterator();

        merkleTree.clear();
        iterator.forEachRemaining(
                record -> merkleTree.updateAdd(record.getKey(), serializationService.toData(record.getValue())));
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    private MerkleTree getMerkleTree(int partitionId, String mapName) {
        EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                .getPartitionContainer(partitionId);
        return partitionContainer.getMerkleTreeOrNull(mapName);
    }
}
