package com.hazelcast.enterprise.wan.operation;

import com.hazelcast.enterprise.wan.EWRDataSerializerHook;
import com.hazelcast.internal.cluster.impl.operations.WanReplicationOperation;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MerkleTreeNodeCompareOperationFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.io.IOException;
import java.util.Map;

/**
 * Operation sent from the source WAN endpoint to the target endpoint.
 * This operation triggers comparison of merkle tree nodes. The result of
 * the comparison is an instance of {@link MerkleTreeNodeValueComparison}.
 * <p>
 * The comparison is offloaded to the {@link #EXECUTOR_NAME} executor.
 */
public class WanMerkleTreeNodeCompareOperation extends Operation
        implements WanReplicationOperation, IdentifiedDataSerializable {
    private static final String EXECUTOR_NAME = ExecutionService.ASYNC_EXECUTOR;
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
     * to the {@link #EXECUTOR_NAME} executor.
     */
    private final class OffloadedMerkleTreeComparison extends Offload implements Runnable {
        private OffloadedMerkleTreeComparison() {
            super(WanMerkleTreeNodeCompareOperation.this);
        }

        @Override
        public void start() {
            nodeEngine.getExecutionService()
                      .getExecutor(EXECUTOR_NAME)
                      .execute(this);
        }

        @Override
        public void run() {
            try {
                MerkleTreeNodeCompareOperationFactory factory = new MerkleTreeNodeCompareOperationFactory(mapName, remoteLevels);
                OperationService os = getNodeEngine().getOperationService();
                Map<Integer, int[]> diffNodes = os.invokeOnPartitions(
                        MapService.SERVICE_NAME, factory, remoteLevels.getPartitionIds());
                MerkleTreeNodeValueComparison comparison = new MerkleTreeNodeValueComparison(diffNodes);

                WanMerkleTreeNodeCompareOperation.this.sendResponse(comparison);
            } catch (Exception e) {
                WanMerkleTreeNodeCompareOperation.this.sendResponse(e);
            }
        }

    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EWRDataSerializerHook.WAN_MERKLE_TREE_NODE_COMPARE_OPERATION;
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
