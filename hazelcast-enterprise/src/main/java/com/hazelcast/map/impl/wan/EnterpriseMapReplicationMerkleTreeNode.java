package com.hazelcast.map.impl.wan;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.map.impl.MerkleTreeNodeEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.DistributedServiceWanEventCounters;

import java.io.IOException;

/**
 * WAN replication object for merkle tree sync requests.
 * This object contains all entries for a single merkle tree node.
 *
 * @see com.hazelcast.wan.merkletree.MerkleTree
 */
public class EnterpriseMapReplicationMerkleTreeNode extends EnterpriseMapReplicationObject {
    private MerkleTreeNodeEntries entries;
    private transient int partitionId;

    public EnterpriseMapReplicationMerkleTreeNode() {
    }

    public EnterpriseMapReplicationMerkleTreeNode(String mapName,
                                                  MerkleTreeNodeEntries entries,
                                                  int partitionId) {
        super(mapName, 0);
        this.entries = entries;
        this.partitionId = partitionId;
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

    @Override
    public Data getKey() {
        // for all purposes, the first key is sufficient
        return entries.getNodeEntries().iterator().next().getKey();
    }

    /**
     * Returns the number of map entries for this merkle tree node.
     */
    public int getEntryCount() {
        return entries.getNodeEntries().size();
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.MAP_REPLICATION_MERKLE_TREE_NODE;
    }

    @Override
    public void incrementEventCount(DistributedServiceWanEventCounters counters) {
        counters.incrementSync(getMapName(), getEntryCount());
    }
}
