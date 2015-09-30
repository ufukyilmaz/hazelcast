package com.hazelcast.cache.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringShutdown;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 03/06/14
 */
public final class CacheDestroyOperation
        extends AbstractOperation
        implements PartitionAwareOperation, AllowedDuringShutdown {

    private final CountDownLatch done = new CountDownLatch(1);
    private final String name;

    public CacheDestroyOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        try {
            int partitionId = getPartitionId();
            EnterpriseCacheService service = getService();
            CachePartitionSegment segment = service.getSegment(partitionId);
            segment.deleteRecordStore(name);
        } finally {
            done.countDown();
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public boolean isUrgent() {
        return true;
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return done.await(timeout, unit);
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

