package com.hazelcast.map.impl.wan;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.MutationObserver;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import javax.annotation.Nonnull;

/**
 * {@link MutationObserver} instance, which updates the provided
 * {@link MerkleTree} when updating the observed {@link RecordStore}
 * <p>
 * In this class we need the Java objects to be converted to
 * {@link Data} in order to guarantee that the same hashcode
 * is generated for the same payload in every mutation case.
 * Without this conversion and with {@link InMemoryFormat#OBJECT},
 * for example {@link #onUpdateRecord} passes a Java object as
 * oldValue and a {@link HeapData} as newValue to the {@link
 * MerkleTree}. These two values generate different hash codes
 * even if they represent the "same" value. These different hash
 * codes lead to changing the corresponding branch of the Merkle
 * tree even if the record is updated with the same value, which
 * is seen by a false difference by the Merkle consistency check.
 *
 * @param <R> The type of the records in the {@link RecordStore}
 */
public class MerkleTreeUpdaterMutationObserver<R extends Record>
        implements MutationObserver<R> {

    private final MerkleTree merkleTree;
    private final SerializationService serializationService;

    /**
     * Creates a {@link MerkleTreeUpdaterMutationObserver} instance
     *
     * @param merkleTree           The Merkle tree to update
     * @param serializationService The serialization service used for
     *                             {@link Data} conversion
     */
    public MerkleTreeUpdaterMutationObserver(MerkleTree merkleTree,
                                             SerializationService serializationService) {
        this.merkleTree = merkleTree;
        this.serializationService = serializationService;
    }

    @Override
    public void onPutRecord(@Nonnull Data key, R record, Object oldValue, boolean backup) {
        updateAdd(key, record);
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, R record, boolean populateIndex) {
        updateAdd(key, record);
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull R record,
                               Object oldValue, Object newValue, boolean backup) {
        updateReplace(key, oldValue, newValue);
    }

    @Override
    public void onRemoveRecord(Data key, R record) {
        updateRemove(key, record);
    }

    @Override
    public void onEvictRecord(Data key, R record) {
        updateRemove(key, record);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull R record, boolean backup) {
        updateAdd(key, record);
    }

    @Override
    public void onReset() {
        clear();
    }

    @Override
    public void onClear() {
        clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        clear();
    }

    private void updateAdd(@Nonnull Data key, R record) {
        merkleTree.updateAdd(key, asData(record.getValue()));
    }

    private void updateRemove(Data key, R record) {
        merkleTree.updateRemove(key, asData(record.getValue()));
    }

    private void updateReplace(@Nonnull Data key, Object oldValue, Object newValue) {
        merkleTree.updateReplace(key, asData(oldValue), asData(newValue));
    }

    private void clear() {
        merkleTree.clear();
    }

    private Data asData(Object value) {
        return serializationService.toData(value);
    }
}
