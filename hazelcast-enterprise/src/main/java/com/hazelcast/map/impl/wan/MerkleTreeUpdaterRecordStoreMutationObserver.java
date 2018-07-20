package com.hazelcast.map.impl.wan;

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.RecordStoreMutationObserver;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.merkletree.MerkleTree;

/**
 * {@link RecordStoreMutationObserver} instance, which updates the provided
 * {@link MerkleTree} when updating the observed {@link RecordStore}
 *
 * @param <R> The type of the records in the {@link RecordStore}
 */
public class MerkleTreeUpdaterRecordStoreMutationObserver<R extends Record> implements RecordStoreMutationObserver<R> {
    private MerkleTree merkleTree;

    /**
     * Creates a {@link MerkleTreeUpdaterRecordStoreMutationObserver} instance
     *
     * @param merkleTree The Merkle tree to update
     */
    public MerkleTreeUpdaterRecordStoreMutationObserver(MerkleTree merkleTree) {
        this.merkleTree = merkleTree;
    }

    @Override
    public void onClear() {
        merkleTree.clear();
    }

    @Override
    public void onPutRecord(Data key, R record) {
        merkleTree.updateAdd(key, record.getValue());
    }

    @Override
    public void onReplicationPutRecord(Data key, R record) {
        onPutRecord(key, record);
    }

    @Override
    public void onUpdateRecord(Data key, R record, Object newValue) {
        merkleTree.updateReplace(key, record.getValue(), newValue);
    }

    @Override
    public void onRemoveRecord(Data key, R record) {
        merkleTree.updateRemove(key, record.getValue());
    }

    @Override
    public void onEvictRecord(Data key, R record) {
        merkleTree.updateRemove(key, record.getValue());
    }

    @Override
    public void onDestroy(boolean internal) {
        merkleTree.clear();
    }

    @Override
    public void onReset() {
        merkleTree.clear();
    }
}
