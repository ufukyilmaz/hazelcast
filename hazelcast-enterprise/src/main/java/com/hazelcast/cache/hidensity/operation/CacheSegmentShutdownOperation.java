package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.EnterpriseCacheService;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.memory.HazelcastMemoryManager;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 20/03/14
 */
public final class CacheSegmentShutdownOperation
        extends Operation
        implements PartitionAwareOperation, AllowedDuringPassiveState {

    private final CountDownLatch done = new CountDownLatch(1);

    @Override
    public void run() throws Exception {
        try {
            EnterpriseSerializationService serializationService
                    = (EnterpriseSerializationService) getNodeEngine().getSerializationService();
            HazelcastMemoryManager memoryManager = serializationService.getMemoryManager();
            if (memoryManager == null || memoryManager.isDisposed()) {
                // otherwise will cause a SIGSEGV
                return;
            }
            int partitionId = getPartitionId();
            EnterpriseCacheService service = getService();
            CachePartitionSegment segment = service.getSegment(partitionId);
            segment.shutdown();
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
