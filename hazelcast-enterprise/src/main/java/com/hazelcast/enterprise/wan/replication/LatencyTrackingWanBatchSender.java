package com.hazelcast.enterprise.wan.replication;

import com.hazelcast.enterprise.wan.BatchWanReplicationEvent;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin.LatencyProbe;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;

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
    private final ConstructorFunction<Address, LatencyProbe> createLatencyProbe
            = new ConstructorFunction<Address, LatencyProbe>() {
        @Override
        public LatencyProbe createNew(Address address) {
            return storeLatencyPlugin.newProbe(KEY, wanPublisherId, address.toString());
        }
    };

    public LatencyTrackingWanBatchSender(WanBatchSender delegate, StoreLatencyPlugin plugin, String wanPublisherId) {
        this.delegate = delegate;
        this.storeLatencyPlugin = plugin;
        this.latencyProbes = new ConcurrentHashMap<Address, LatencyProbe>();
        this.wanPublisherId = wanPublisherId;
    }

    @Override
    public boolean send(BatchWanReplicationEvent batchReplicationEvent, Address target) {
        final long startNanos = System.nanoTime();
        try {
            return delegate.send(batchReplicationEvent, target);
        } finally {
            final LatencyProbe probe = ConcurrencyUtil.getOrPutIfAbsent(latencyProbes, target, createLatencyProbe);
            probe.recordValue(System.nanoTime() - startNanos);
        }
    }
}
