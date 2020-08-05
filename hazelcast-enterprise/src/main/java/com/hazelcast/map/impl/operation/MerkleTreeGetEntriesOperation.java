package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.internal.util.collection.InflatableSet.Builder;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.map.impl.EnterprisePartitionContainer;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.wan.impl.merkletree.MerkleTree;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import static com.hazelcast.map.impl.EntryViews.createWanEntryView;
import static com.hazelcast.wan.impl.merkletree.MerkleTreeUtil.getLeafOrderForHash;
import static com.hazelcast.wan.impl.merkletree.MerkleTreeUtil.getLevelOfNode;

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
    protected void runInternal() {
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

        Int2ObjectHashMap<Builder<WanMapEntryView<Object, Object>>> entryBuilders = new Int2ObjectHashMap<>(
                MapUtil.calculateInitialCapacity(merkleTreeOrderValuePairs.length / 2), MapUtil.HASHMAP_DEFAULT_LOAD_FACTOR);
        for (int i = 0; i < merkleTreeOrderValuePairs.length; i += 2) {
            entryBuilders.put(merkleTreeOrderValuePairs[i], InflatableSet.newBuilder(1));
        }

        // here we assume all nodes are on the same level
        int levelOfRequestedNodes = getLevelOfNode(merkleTreeOrderValuePairs[0]);

        recordStore.iterator()
                   .forEachRemaining(entry -> {
                       Data keyData = entry.getKey();
                       int keyHash = keyData.hashCode();
                       int currentKeyNodeOrder = getLeafOrderForHash(keyHash, levelOfRequestedNodes);
                       Builder<WanMapEntryView<Object, Object>> entriesBuilder = entryBuilders.get(currentKeyNodeOrder);
                       if (entriesBuilder != null) {
                           Record record = entry.getValue();
                           entriesBuilder.add(createWanEntryView(
                                   keyData, mapServiceContext.toData(record.getValue()), record,
                                   getNodeEngine().getSerializationService()));
                       }
                   });


        result = new ArrayList<>(merkleTreeOrderValuePairs.length / 2);
        for (Entry<Integer, Builder<WanMapEntryView<Object, Object>>> builderEntry : entryBuilders.entrySet()) {
            result.add(new MerkleTreeNodeEntries(builderEntry.getKey(), builderEntry.getValue().build()));
        }
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
    public int getClassId() {
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
