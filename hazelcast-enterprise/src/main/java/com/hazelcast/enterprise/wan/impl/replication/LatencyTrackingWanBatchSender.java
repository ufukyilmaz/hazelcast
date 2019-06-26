package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

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
    public ICompletableFuture<Boolean> send(BatchWanReplicationEvent batchReplicationEvent,
                                            final Address target) {
        final long startNanos = System.nanoTime();
        ICompletableFuture<Boolean> result = delegate.send(batchReplicationEvent, target);
        result.andThen(new ExecutionCallback<Boolean>() {
            @Override
            public void onResponse(Boolean response) {
                recordLatency(target, startNanos);
            }

            @Override
            public void onFailure(Throwable t) {
                recordLatency(target, startNanos);
            }
        }, responseExecutor);
        return result;
    }

    private void recordLatency(Address target, long startNanos) {
        LatencyProbe probe = ConcurrencyUtil.getOrPutIfAbsent(latencyProbes, target, createLatencyProbe);
        probe.recordValue(System.nanoTime() - startNanos);
    }
}
