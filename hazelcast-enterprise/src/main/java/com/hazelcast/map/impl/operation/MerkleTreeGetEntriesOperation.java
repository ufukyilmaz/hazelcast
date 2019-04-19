package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.util.collection.InflatableSet;
import com.hazelcast.util.collection.InflatableSet.Builder;
import com.hazelcast.wan.merkletree.MerkleTree;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

/**
 * Operation for fetching map entries for any number of merkle tree nodes.
 *
 * @see MerkleTree
 * @since 3.11
 */
public class MerkleTreeGetEntriesOperation extends MapOperation implements ReadonlyOperation {
    private int[] merkleTreeOrderValuePairs;
    private Collection<MerkleTreeNodeEntries> result;

    public MerkleTreeGetEntriesOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MerkleTreeGetEntriesOperation(String mapName, int[] merkleTreeOrderValuePairs) {
        super(mapName);
        this.merkleTreeOrderValuePairs = merkleTreeOrderValuePairs;
    }

    @Override
    public void run() throws Exception {
        int partitionId = getPartitionId();
        EnterprisePartitionContainer partitionContainer = (EnterprisePartitionContainer) mapServiceContext
                .getPartitionContainer(partitionId);
        MerkleTree localMerkleTree = partitionContainer.getMerkleTreeOrNull(getName());

        if (localMerkleTree == null
                || merkleTreeOrderValuePairs == null
                || merkleTreeOrderValuePairs.length == 0) {
            result = Collections.emptyList();
            return;
        }

        result = new ArrayList<MerkleTreeNodeEntries>(merkleTreeOrderValuePairs.length / 2);
        for (int i = 0; i < merkleTreeOrderValuePairs.length; i += 2) {
            int order = merkleTreeOrderValuePairs[i];
            final Builder<EntryView<Data, Data>> entriesBuilder = InflatableSet.newBuilder(1);

            localMerkleTree.forEachKeyOfNode(order, new Consumer<Object>() {
                @Override
                public void accept(Object key) {
                    Record record = recordStore.getRecord((Data) key);
                    entriesBuilder.add(createSimpleEntryView(record));
                }
            });
            result.add(new MerkleTreeNodeEntries(order, entriesBuilder.build()));
        }
    }

    /**
     * Creates an entry view for the given record.
     *
     * @param record a map entry record
     * @return the map entry view
     */
    private SimpleEntryView<Data, Data> createSimpleEntryView(Record record) {
        return new SimpleEntryView<Data, Data>()
                .withKey(mapServiceContext.toData(record.getKey()))
                .withValue(mapServiceContext.toData(record.getValue()))
                .withVersion(record.getVersion())
                .withHits(record.getHits())
                .withLastAccessTime(record.getLastAccessTime())
                .withLastUpdateTime(record.getLastUpdateTime())
                .withTtl(record.getTtl())
                .withMaxIdle(record.getMaxIdle())
                .withCreationTime(record.getCreationTime())
                .withExpirationTime(record.getExpirationTime())
                .withLastStoredTime(record.getLastStoredTime());
    }

    @Override
    public Object getResponse() {
        return result;
    }


    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.MERKLE_TREE_GET_ENTRIES_OPERATION;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        merkleTreeOrderValuePairs = in.readIntArray();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeIntArray(merkleTreeOrderValuePairs);
    }
}
