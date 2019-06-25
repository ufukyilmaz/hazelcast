package com.hazelcast.enterprise.wan.impl.sync;

import com.hazelcast.enterprise.wan.WanSyncType;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.SetUtil;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;

/**
 * Base class for WAN anti-entropy related events.
 */
public abstract class WanAntiEntropyEvent implements IdentifiedDataSerializable {
    /**
     * The unique ID of the WAN anti-entropy events. Used to distinguish
     * between separate anti-entropy requests.
     */
    protected UUID uuid;

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
        assignUuid();
        this.mapName = mapName;
    }

    protected WanAntiEntropyEvent(UUID uuid) {
        this.uuid = uuid;
    }

    protected WanAntiEntropyEvent(UUID uuid, String mapName) {
        this.uuid = uuid;
        this.mapName = mapName;
    }

    protected void assignUuid() {
        this.uuid = UuidUtil.newUnsecureUUID();
    }

    public UUID getUuid() {
        return uuid;
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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeUUID(out, uuid);
        out.writeUTF(mapName);
        out.writeInt(partitionSet.size());
        for (Integer partitionId : partitionSet) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = readUUID(in);
        mapName = in.readUTF();
        int size = in.readInt();
        partitionSet = SetUtil.createHashSet(size);
        for (int i = 0; i < size; i++) {
            partitionSet.add(in.readInt());
        }
    }
}
