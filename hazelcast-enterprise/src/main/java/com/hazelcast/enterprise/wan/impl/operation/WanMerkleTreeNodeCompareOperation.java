package com.hazelcast.enterprise.wan.impl.operation;

import com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher;
import com.hazelcast.internal.cluster.impl.operations.WanOperation;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MerkleTreeNodeCompareOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static com.hazelcast.enterprise.wan.impl.replication.WanBatchPublisher.WAN_EXECUTOR;

/**
 * Operation sent from the source WAN endpoint to the target endpoint.
 * This operation triggers comparison of merkle tree nodes. The result of
 * the comparison is an instance of {@link MerkleTreeNodeValueComparison}.
 * <p>
 * The comparison is offloaded to the {@link WanBatchPublisher#WAN_EXECUTOR} wanExecutor.
 */
public class WanMerkleTreeNodeCompareOperation extends Operation
        implements WanOperation, IdentifiedDataSerializable, AllowedDuringPassiveState {
    private String mapName;
    private MerkleTreeNodeValueComparison remoteLevels;

    public WanMerkleTreeNodeCompareOperation() {
    }

    public WanMerkleTreeNodeCompareOperation(String mapName,
                                             MerkleTreeNodeValueComparison remoteLevels) {
        this.mapName = mapName;
        this.remoteLevels = remoteLevels;
    }

    @Override
    public CallStatus call() {
        return new OffloadedMerkleTreeComparison();
    }

    /**
     * A task for fetching local map merkle tree node values and comparing
     * them with the remote values.
     * The invocations for fetching local merkle tree values is offloaded
     * to the {@link WanBatchPublisher#WAN_EXECUTOR} wanExecutor.
     */
    private final class OffloadedMerkleTreeComparison extends Offload implements BiConsumer<Map<Integer, int[]>, Throwable> {

        private OffloadedMerkleTreeComparison() {
            super(WanMerkleTreeNodeCompareOperation.this);
        }

        @Override
        public void start() {
            try {
                MerkleTreeNodeCompareOperationFactory factory = new MerkleTreeNodeCompareOperationFactory(mapName, remoteLevels);
                OperationService os = getNodeEngine().getOperationService();
                Executor wanExecutor = nodeEngine.getExecutionService().getExecutor(WAN_EXECUTOR);
                os.<int[]>invokeOnPartitionsAsync(MapService.SERVICE_NAME, factory, remoteLevels.getPartitionIds())
                        .whenCompleteAsync(this, wanExecutor);
            } catch (Exception e) {
                WanMerkleTreeNodeCompareOperation.this.sendResponse(e);
            }
        }

        @Override
        public void accept(Map<Integer, int[]> response, Throwable throwable) {
            if (throwable == null) {
                MerkleTreeNodeValueComparison comparison = new MerkleTreeNodeValueComparison(response);
                WanMerkleTreeNodeCompareOperation.this.sendResponse(comparison);
            } else {
                WanMerkleTreeNodeCompareOperation.this.sendResponse(throwable);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_MERKLE_TREE_NODE_COMPARE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeObject(remoteLevels);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        remoteLevels = in.readObject();
    }


}
