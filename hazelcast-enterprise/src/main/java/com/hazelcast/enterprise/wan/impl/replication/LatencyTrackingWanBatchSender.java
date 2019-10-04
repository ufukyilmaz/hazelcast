package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.InternalCompletableFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * A wrapper around a {@link WanBatchSender} implementation that additionally tracks
 * the latency for all successful transmissions of the WAN batches.
 */
public class LatencyTrackingWanBatchSender implements WanBatchSender {
    static final String KEY = "WanBatchSenderLatency";
    private final WanBatchSender delegate;
    private final StoreLatencyPlugin storeLatencyPlugin;
    private final ConcurrentHashMap<Address, LatencyProbe> latencyProbes;
    private final String wanPublisherId;
    private final Executor responseExecutor;
    private final ConstructorFunction<Address, LatencyProbe> createLatencyProbe
            = new ConstructorFunction<Address, LatencyProbe>() {
        @Override
        public LatencyProbe createNew(Address address) {
            return storeLatencyPlugin.newProbe(KEY, wanPublisherId, address.toString());
        }
    };

    public LatencyTrackingWanBatchSender(WanBatchSender delegate,
                                         StoreLatencyPlugin plugin,
                                         String wanPublisherId,
                                         Executor responseExecutor) {
        this.delegate = delegate;
        this.storeLatencyPlugin = plugin;
        this.latencyProbes = new ConcurrentHashMap<Address, LatencyProbe>();
        this.wanPublisherId = wanPublisherId;
        this.responseExecutor = responseExecutor;
    }

    @Override
    public void init(Node node, WanBatchReplication publisher) {
        throw new UnsupportedOperationException(
                "Not supported as a standalone sender, use constructor for initialisation");
    }

    @Override
    public InternalCompletableFuture<Boolean> send(BatchWanReplicationEvent batchReplicationEvent,
                                                   final Address target) {
        final long startNanos = System.nanoTime();
        InternalCompletableFuture<Boolean> result = delegate.send(batchReplicationEvent, target);
        result.whenCompleteAsync((response, t) -> recordLatency(target, startNanos), responseExecutor);
        return result;
    }

    private void recordLatency(Address target, long startNanos) {
        LatencyProbe probe = ConcurrencyUtil.getOrPutIfAbsent(latencyProbes, target, createLatencyProbe);
        probe.recordValue(System.nanoTime() - startNanos);
    }
}
