package com.hazelcast.enterprise.wan.sync;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.Collections;
import java.util.Set;

/**
 * Base class for WAN anti-entropy related events.
 */
public abstract class WanAntiEntropyEvent implements IdentifiedDataSerializable {
    /**
     * The name of the map, can be {@code null} in case of
     * {@link WanSyncType#ALL_MAPS}
     */
    protected String mapName;
    /** The partitions to be synced. If empty, all partitions will be synced */
    protected Set<Integer> partitionSet = Collections.emptySet();

    /**
     * The operation which should receive the {@link WanAntiEntropyEventResult}.
     */
    private transient WanAntiEntropyEventPublishOperation op;

    private transient WanAntiEntropyEventResult processingResult;

    @SuppressWarnings("unused")
    public WanAntiEntropyEvent() {
    }

    public WanAntiEntropyEvent(String mapName) {
        this.mapName = mapName;
    }

    public String getMapName() {
        return mapName;
    }

    public Set<Integer> getPartitionSet() {
        return partitionSet;
    }

    public void setPartitionSet(Set<Integer> partitionSet) {
        this.partitionSet = partitionSet;
    }

    public WanAntiEntropyEventPublishOperation getOp() {
        return op;
    }

    public void setOp(WanAntiEntropyEventPublishOperation op) {
        this.op = op;
    }

    public WanAntiEntropyEventResult getProcessingResult() {
        return processingResult;
    }

    public void setProcessingResult(WanAntiEntropyEventResult processingResult) {
        this.processingResult = processingResult;
    }

    /**
     * Sends the response with the {@link #getProcessingResult() processing result}
     * to the event sender.
     */
    public void sendResponse() {
        try {
            op.sendResponse(processingResult);
        } catch (Exception ex) {
            op.getNodeEngine().getLogger(WanAntiEntropyEvent.class).warning(ex);
        }
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    /**
     * Clones this object with all data except the partition keys the event
     * applies to.
     *
     * @return a cloned instance
     */
    public abstract WanAntiEntropyEvent cloneWithoutPartitionKeys();
}
