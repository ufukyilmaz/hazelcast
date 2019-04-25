package com.hazelcast.map.impl.query;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartition;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook.F_ID;
import static com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook.QUERY_OP;
import static com.hazelcast.spi.CallStatus.DONE_RESPONSE;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Query operation that runs a query for the HD memory.
 *
 * It is run on a generic thread, and then it spawns partition-query-operations
 * for each local partition. This operation does not return any result until all
 * the partition-query-operations have finished. Then it return a merged result.
 *
 * The main reason for this design is that we want the partition-query-operations
 * to be local. In this way we skip the network overhead. So each member gets a
 * single HDQueryOperation and then it queries all local partitions on partitions
 * threads.
 */
public class HDQueryOperation extends MapOperation implements ReadonlyOperation {

    private Query query;
    private transient Result result;

    public HDQueryOperation() {
    }

    public HDQueryOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    @Override
    public CallStatus call() {
        QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());

        BitSet localPartitions = localPartitions();
        if (localPartitions.cardinality() == 0) {
            // important to deal with situation of not having any partitions
            this.result = queryRunner.populateEmptyResult(query, Collections.emptyList());
            return DONE_RESPONSE;
        }

        return new OffloadedImpl(queryRunner, localPartitions);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    private int partitionCount() {
        return getNodeEngine().getPartitionService().getPartitionCount();
    }

    private OperationServiceImpl getOperationService() {
        return (OperationServiceImpl) getNodeEngine().getOperationService();
    }

    private BitSet localPartitions() {
        BitSet partitions = new BitSet(partitionCount());
        for (IPartition partition : getNodeEngine().getPartitionService().getPartitions()) {
            if (partition.isLocal()) {
                partitions.set(partition.getPartitionId());
            }
        }

        return partitions;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        // This is required since if the returnsResponse() method returns false
        // there won't be any response sent to the invoking party - this means
        // that the operation won't be retried if the exception is instanceof
        // HazelcastRetryableException
        sendResponse(e);
    }

    @Override
    public int getId() {
        return QUERY_OP;
    }

    @Override
    public final int getFactoryId() {
        return F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(query);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        query = in.readObject();
    }

    private final class OffloadedImpl extends Offload {
        private final BitSet localPartitions;
        private final QueryRunner queryRunner;

        private OffloadedImpl(QueryRunner queryRunner, BitSet localParitions) {
            super(HDQueryOperation.this);
            this.localPartitions = localParitions;
            this.queryRunner = queryRunner;
        }

        @Override
        public void start() {
            QueryFuture future = new QueryFuture(localPartitions.cardinality());
            getOperationService().executeOnPartitions(new QueryTaskFactory(query, future), localPartitions);
            future.andThen(new ExecutionCallbackImpl(queryRunner, query));
        }
    }

    private class QueryFuture extends AbstractCompletableFuture {
        private final AtomicReferenceArray<Result> resultArray = new AtomicReferenceArray<>(partitionCount());
        private final AtomicInteger remaining;

        QueryFuture(int localPartitionCount) {
            super(getNodeEngine(), getLogger());
            this.remaining = new AtomicInteger(localPartitionCount);
        }

        void addResult(int partitionId, Result result) {
            if (result != null) {
                resultArray.set(partitionId, result);
            }

            if (remaining.decrementAndGet() == 0) {
                setResult(resultArray);
            }
        }

        void completeExceptionally(Throwable error) {
            super.setResult(error);
        }
    }

    private class QueryTaskFactory implements PartitionTaskFactory {
        private final Query query;
        private final QueryFuture future;

        QueryTaskFactory(Query query, QueryFuture future) {
            this.query = query;
            this.future = future;
        }

        @Override
        public Object create(int partitionId) {
            return new QueryTask(query, partitionId, future);
        }
    }

    private class QueryTask implements Runnable {
        private final Query query;
        private final int partitionId;
        private final QueryFuture future;

        QueryTask(Query query, int partitionId, QueryFuture future) {
            this.query = query;
            this.partitionId = partitionId;
            this.future = future;
        }

        @Override
        public void run() {
            IPartition partition = getNodeEngine().getPartitionService().getPartition(partitionId);
            if (!partition.isLocal()) {
                future.addResult(partitionId, null);
                return;
            }

            try {
                QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(query.getMapName());
                Result result = queryRunner.runPartitionIndexOrPartitionScanQueryOnGivenOwnedPartition(query, partitionId);
                future.addResult(partitionId, result);
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
        }
    }

    private class ExecutionCallbackImpl implements ExecutionCallback<AtomicReferenceArray<Result>> {
        private final QueryRunner queryRunner;
        private final Query query;

        ExecutionCallbackImpl(QueryRunner queryRunner, Query query) {
            this.queryRunner = queryRunner;
            this.query = query;
        }

        @Override
        public void onResponse(AtomicReferenceArray<Result> response) {
            try {
                Result combinedResult = queryRunner.populateEmptyResult(query, Collections.emptyList());
                populateResult(response, combinedResult);
                HDQueryOperation.this.sendResponse(combinedResult);
            } catch (Exception e) {
                HDQueryOperation.this.sendResponse(e);
                throw rethrow(e);
            }
        }

        private void populateResult(AtomicReferenceArray<Result> resultArray, Result combinedResult) {
            for (int k = 0; k < resultArray.length(); k++) {
                Result partitionResult = resultArray.get(k);
                if (partitionResult != null) {
                    combinedResult.combine(partitionResult);
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            HDQueryOperation.this.sendResponse(t);
        }
    }
}
