package com.hazelcast.map.impl.wan;

import com.hazelcast.core.EntryView;
import com.hazelcast.enterprise.wan.impl.operation.WanDataSerializerHook;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.wan.WanEventCounters;
import com.hazelcast.wan.WanEventType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * WAN replication object for merkle tree sync requests.
 * This object contains all entries for a single merkle tree node.
 *
 * @see com.hazelcast.wan.impl.merkletree.MerkleTree
 */
public class WanEnterpriseMapMerkleTreeNode extends WanEnterpriseMapEvent<Collection<EntryView<Object, Object>>> {
    private transient UUID uuid;
    private MerkleTreeNodeEntries entries;
    private transient int partitionId;

    public WanEnterpriseMapMerkleTreeNode() {
    }

    public WanEnterpriseMapMerkleTreeNode(UUID uuid, String mapName, MerkleTreeNodeEntries entries, int partitionId) {
        super(mapName, 0);
        this.uuid = uuid;
        this.entries = entries;
        this.partitionId = partitionId;
    }

    public UUID getUuid() {
        return uuid;
    }

    public MerkleTreeNodeEntries getEntries() {
        return entries;
    }

    public void setEntries(MerkleTreeNodeEntries entries) {
        this.entries = entries;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(entries);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        entries = in.readObject();
    }

    @Nonnull
    @Override
    public Data getKey() {
        // for all purposes, the first key is sufficient
        return entries.getNodeEntries().iterator().next().getDataKey();
    }

    /**
     * Returns the number of map entries for this merkle tree node.
     */
    public int getEntryCount() {
        return entries.getNodeEntries().size();
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.MAP_REPLICATION_MERKLE_TREE_NODE;
    }

    @Override
    public void incrementEventCount(@Nonnull WanEventCounters counters) {
        counters.incrementSync(getMapName(), getEntryCount());
    }

    @Nonnull
    @Override
    public WanEventType getEventType() {
        return WanEventType.SYNC;
    }

    @Nullable
    @Override
    @SuppressWarnings("unchecked")
    public Collection<EntryView<Object, Object>> getEventObject() {
        // upcast to allow Set<WanMapEntryView<Object, Object>> to be returned as
        // Collection<EntryView<Object, Object>> and avoid copying
        return (Set) getEntries().getNodeEntries();
    }

}
