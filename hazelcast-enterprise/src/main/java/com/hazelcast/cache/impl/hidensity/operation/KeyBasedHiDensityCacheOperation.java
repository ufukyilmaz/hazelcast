package com.hazelcast.cache.impl.hidensity.operation;

import com.hazelcast.cache.impl.operation.MutableOperation;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;

import java.io.IOException;

abstract class KeyBasedHiDensityCacheOperation extends HiDensityCacheOperation {

    protected Data key;

    protected KeyBasedHiDensityCacheOperation() {
    }

    protected KeyBasedHiDensityCacheOperation(String name) {
        this(name, null, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected KeyBasedHiDensityCacheOperation(String name, Data key) {
        this(name, key, MutableOperation.IGNORE_COMPLETION, false);
    }

    protected KeyBasedHiDensityCacheOperation(String name, Data key,
                                              boolean dontCreateCacheRecordStoreIfNotExist) {
        this(name, key, MutableOperation.IGNORE_COMPLETION, dontCreateCacheRecordStoreIfNotExist);
    }

    protected KeyBasedHiDensityCacheOperation(String name, Data key, int completionId) {
        this(name, key, completionId, false);
    }

    protected KeyBasedHiDensityCacheOperation(String name, Data key, int completionId,
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
        IOUtil.writeData(out, key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = readNativeMemoryOperationData(in);
    }

}
