package com.hazelcast.map.impl.wan;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.RecordStoreMutationObserver;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

/**
 * {@link RecordStoreMutationObserver} instance, which updates the provided
 * {@link MerkleTree} when updating the observed {@link RecordStore}
 * <p>
 * In this class we need the Java objects to be converted to {@link Data}
 * in order to guarantee that the same hashcode is generated for the same
 * payload in every mutation case. Without this conversion and with
 * {@link InMemoryFormat#OBJECT}, for example {@link #onUpdateRecord} passes
 * a Java object as oldValue and a {@link HeapData} as newValue to the
 * {@link MerkleTree}. These two values generate different hash codes even
 * if they represent the "same" value. These different hash codes lead to
 * changing the corresponding branch of the Merkle tree even if the record
 * is updated with the same value, which is seen by a false difference by
 * the Merkle consistency check.
 *
 * @param <R> The type of the records in the {@link RecordStore}
 */
public class MerkleTreeUpdaterRecordStoreMutationObserver<R extends Record> implements RecordStoreMutationObserver<R> {
    private final MerkleTree merkleTree;
    private final SerializationService serializationService;

    /**
     * Creates a {@link MerkleTreeUpdaterRecordStoreMutationObserver} instance
     *
     * @param merkleTree           The Merkle tree to update
     * @param serializationService The serialization service used for
     *                             {@link Data} conversion
     */
    public MerkleTreeUpdaterRecordStoreMutationObserver(MerkleTree merkleTree, SerializationService serializationService) {
        this.merkleTree = merkleTree;
        this.serializationService = serializationService;
    }

    @Override
    public void onClear() {
        merkleTree.clear();
    }

    @Override
    public void onPutRecord(Data key, R record) {
        merkleTree.updateAdd(key, asData(record.getValue()));
    }

    @Override
    public void onReplicationPutRecord(Data key, R record) {
        onPutRecord(key, record);
    }

    @Override
    public void onUpdateRecord(Data key, R record, Object newValue) {
        merkleTree.updateReplace(key, asData(record.getValue()), asData(newValue));
    }

    @Override
    public void onRemoveRecord(Data key, R record) {
        merkleTree.updateRemove(key, asData(record.getValue()));
    }

    @Override
    public void onEvictRecord(Data key, R record) {
        merkleTree.updateRemove(key, asData(record.getValue()));
    }

    @Override
    public void onLoadRecord(Data key, R record) {
        onPutRecord(key, record);
    }

    @Override
    public void onDestroy(boolean internal) {
        merkleTree.clear();
    }

    @Override
    public void onReset() {
        merkleTree.clear();
    }

    private Data asData(Object value) {
        return serializationService.toData(value);
    }
}
