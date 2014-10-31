package com.hazelcast.cache.enterprise.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.enterprise.BreakoutCacheRecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.WrongTargetException;

import java.io.IOException;

/**
 * @author mdogan 11/02/14
 */
public final class CacheEvictionOperation
        extends AbstractOperation
        implements PartitionAwareOperation {

    final String name;
    final int percentage;

    public CacheEvictionOperation(String name, int percentage) {
        this.name = name;
        this.percentage = percentage;
    }

    public void run() throws Exception {
        EnterpriseCacheService cacheService = getService();
        BreakoutCacheRecordStore cache =
                (BreakoutCacheRecordStore) cacheService
                        .getCacheRecordStore(name, getPartitionId());
        if (cache != null) {
            cache.evictExpiredRecords(percentage);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof PartitionMigratingException || e instanceof WrongTargetException) {
            getLogger().finest(e);
        } else {
            super.logError(e);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }
}
