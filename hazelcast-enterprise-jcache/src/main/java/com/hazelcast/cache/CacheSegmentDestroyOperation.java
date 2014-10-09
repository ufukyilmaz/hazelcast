package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 20/03/14
 */
final class CacheSegmentDestroyOperation extends AbstractOperation implements PartitionAwareOperation {

    private final CountDownLatch done = new CountDownLatch(1);

    @Override
    public void run() throws Exception {
        try {
            int partitionId = getPartitionId();
            CacheService service = getService();
            CachePartitionSegment segment = service.getSegment(partitionId);
            segment.destroy();
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

    boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
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

