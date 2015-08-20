package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;

import java.io.IOException;

/**
 * @author sozal 07/08/15
 */
abstract class AbstractKeyBasedHiDensityCacheOperation
        extends AbstractHiDensityCacheOperation {

    protected Data key;

    protected AbstractKeyBasedHiDensityCacheOperation() {
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name) {
        this(name, null, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, boolean dontCreateCacheRecordStore) {
        this(name, null, MutableOperation.IGNORE_COMPLETION, dontCreateCacheRecordStore);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, Data key) {
        this(name, key, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, Data key,
                                                      boolean dontCreateCacheRecordStoreIfNotExist) {
        this(name, key, MutableOperation.IGNORE_COMPLETION, dontCreateCacheRecordStoreIfNotExist);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, int completionId) {
        this(name, null, completionId, false);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, int completionId,
                                                      boolean dontCreateCacheRecordStoreIfNotExist) {
        this(name, null, completionId, dontCreateCacheRecordStoreIfNotExist);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, Data key, int completionId) {
        this(name, key, completionId, false);
    }

    protected AbstractKeyBasedHiDensityCacheOperation(String name, Data key, int completionId,
                                                      boolean dontCreateCacheRecordStoreIfNotExist) {
        super(name, completionId, dontCreateCacheRecordStoreIfNotExist);
        this.key = key;
    }

    @Override
    protected void disposeInternal(EnterpriseSerializationService serializationService) {
        if (key != null) {
            serializationService.disposeData(key);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = readNativeMemoryOperationData(in);
    }

}
