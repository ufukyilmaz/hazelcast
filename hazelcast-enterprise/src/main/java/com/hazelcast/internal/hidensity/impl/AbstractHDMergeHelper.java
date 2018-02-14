package com.hazelcast.internal.hidensity.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.EmptyStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
 * @param <S> partition store of data structure
 */
public abstract class AbstractHDMergeHelper<S> {

    private final Address thisAddress;
    private final OperationExecutor operationExecutor;
    private final IPartitionService partitionService;
    private final ConcurrentMap<Integer, Map<String, S>> storesByPartitionId
            = new ConcurrentHashMap<Integer, Map<String, S>>();

    public AbstractHDMergeHelper(NodeEngine nodeEngine) {
        this.thisAddress = nodeEngine.getThisAddress();
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService()).getOperationExecutor();
        this.partitionService = nodeEngine.getPartitionService();
    }

    /**
     * Collects HD backed stores and removes them from their partition containers.
     * This makes the HD stores inaccessible for partition threads and unmodifiable during merge.
     *
     * @param collectedHdStores collected HD backed stores of the partition
     * @param partitionId       partition id of stores to be collected
     * @return total entry count in all stores independent of the backing storage type
     */
    protected abstract void collectHdStores(Map<String, S> collectedHdStores, int partitionId);

    /**
     * Frees HD space by destroying HD backed store
     */
    protected abstract void destroyStore(S hdStore);

    /**
     * Call this method, if an instance of this class should be prepared for next usage
     */
    public final void prepare() {
        storesByPartitionId.clear();
        collectHdStoresInternal();
    }

    public final Collection<S> getStoresOf(int partitionId) {
        Map<String, S> storesByName = storesByPartitionId.get(partitionId);
        if (isNullOrEmpty(storesByName)) {
            return Collections.emptyList();
        }

        return storesByName.values();
    }

    public final S getOrNullStore(String name, int partitionId) {
        Map<String, S> storesByName = storesByPartitionId.get(partitionId);
        if (isNullOrEmpty(storesByName)) {
            return null;
        }

        return storesByName.get(name);
    }

    /**
     * Frees HD space by destroying HD backed stores upon successful merge
     */
    public void destroyCollectedHdStores() {
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
                            destroyStore(store);
                            iterator.remove();
                        }
                    }
                }
            });
        }
    }

    private void collectHdStoresInternal() {
        List<Integer> ownedPartitionIds = partitionService.getMemberPartitions(thisAddress);

        final CountDownLatch latch = new CountDownLatch(ownedPartitionIds.size());

        for (final Integer ownedPartitionId : ownedPartitionIds) {
            operationExecutor.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return ownedPartitionId;
                }

                @Override
                public void run() {
                    Map<String, S> storesByName = new HashMap<String, S>();

                    collectHdStores(storesByName, ownedPartitionId);

                    if (!isNullOrEmpty(storesByName)) {
                        storesByPartitionId.put(ownedPartitionId, storesByName);
                    }

                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
        }
    }
}
