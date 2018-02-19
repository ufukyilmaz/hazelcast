package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.util.MapUtil.isNullOrEmpty;

/**
 * Contains shared helper functionality for {@link com.hazelcast.spi.SplitBrainHandlerService}
 * of HD backed data structures
 *
 * @param <S> HD backed store of a partition
 */
public abstract class AbstractHDMergeHelper<S> {

    private final IPartitionService partitionService;
    private final OperationExecutor operationExecutor;
    private final ConcurrentMap<Integer, Map<String, S>> storesByPartitionId
            = new ConcurrentHashMap<Integer, Map<String, S>>();

    public AbstractHDMergeHelper(NodeEngine nodeEngine) {
        this.partitionService = nodeEngine.getPartitionService();
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService()).getOperationExecutor();
    }

    /**
     * Iterates over on heap and HD stores.
     */
    protected abstract Iterator<S> storeIterator(int partitionId);

    /**
     * Name of the HD store
     */
    protected abstract String extractHDStoreName(S store);

    /**
     * Frees HD space by destroying HD store
     */
    protected abstract void destroyHDStore(S store);

    /**
     * @return {@code true} if this is an HD store, otherwise return {@code false}
     */
    protected abstract boolean isHDStore(S store);

    /**
     * Call this method, if an instance of this class should be prepared for next usage
     */
    public final void prepare() {
        storesByPartitionId.clear();
        collectOrDestroyHDStores();
    }

    /**
     * - Collects HD backed stores from owner partitions and removes them from their partition containers.
     * This makes HD stores inaccessible for partition threads and also makes them unmodifiable during merge.
     * <p>
     * - Destroys backup HD stores to prevent HD memory leaks
     */
    private void collectOrDestroyHDStores() {
        int partitionCount = partitionService.getPartitionCount();
        final CountDownLatch latch = new CountDownLatch(partitionCount);

        for (int i = 0; i < partitionCount; i++) {
            final int partitionId = i;
            operationExecutor.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    try {
                        collectOrDestroyHDStores(partitionId);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
        }
    }

    private void collectOrDestroyHDStores(int partitionId) {
        Map<String, S> storesByName = new HashMap<String, S>();

        Iterator<S> iterator = storeIterator(partitionId);
        while (iterator.hasNext()) {
            S store = iterator.next();

            if (!isHDStore(store)) {
                continue;
            }

            if (isLocalPartition(partitionId)) {
                // Only collect HD stores owned by this node
                storesByName.put(extractHDStoreName(store), store);
            } else {
                // Destroy HD stores not owned by this node
                destroyHDStore(store);
            }

            iterator.remove();
        }

        if (!isNullOrEmpty(storesByName)) {
            storesByPartitionId.put(partitionId, storesByName);
        }
    }

    private boolean isLocalPartition(int partitionId) {
        IPartition partition = partitionService.getPartition(partitionId, false);
        return partition.isLocal();
    }

    /**
     * Frees HD space by destroying collected HD stores upon finish of merge tasks
     */
    public final void destroyCollectedHDStores() {
        final Map<Integer, Map<String, S>> storesByPartitionId = this.storesByPartitionId;
        Set<Integer> partitionIds = storesByPartitionId.keySet();
        for (final Integer partitionId : partitionIds) {
            operationExecutor.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    Map<String, S> storesByName = storesByPartitionId.get(partitionId);
                    if (!isNullOrEmpty(storesByName)) {
                        Iterator<S> iterator = storesByName.values().iterator();
                        while (iterator.hasNext()) {
                            S store = iterator.next();
                            destroyHDStore(store);
                            iterator.remove();
                        }
                    }
                }
            });
        }
    }

    public final Collection<S> getHDStoresOf(int partitionId) {
        Map<String, S> storesByName = storesByPartitionId.get(partitionId);
        if (isNullOrEmpty(storesByName)) {
            return Collections.emptyList();
        }

        return storesByName.values();
    }

    public final S getOrNullHDStore(String name, int partitionId) {
        Map<String, S> storesByName = storesByPartitionId.get(partitionId);
        if (isNullOrEmpty(storesByName)) {
            return null;
        }

        return storesByName.get(name);
    }
}
