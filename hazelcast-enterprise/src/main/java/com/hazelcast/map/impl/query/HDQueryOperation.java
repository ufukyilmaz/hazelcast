package com.hazelcast.map.impl.query;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EnterpriseMapDataSerializerHook;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.batching.PartitionAwareCallable;
import com.hazelcast.spi.impl.operationservice.impl.batching.PartitionAwareCallableBatchingRunnable;
import com.hazelcast.spi.impl.operationservice.impl.batching.PartitionAwareCallableFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Query operation that runs a query for the HD memory.
 * It is run on a generic thread, and then it spawns partition-query-operations for each local partition.
 * This operation does not return any result until all the partition-query-operations have finished.
 * Then it return a merged result.
 * The main reason for this design is that we want the partition-query-operations to be local. In this way we skip
 * the network overhead. So each member gets a single HDQueryOperation and then it queries all local partitions
 * on partitions threads.
 */
public class HDQueryOperation extends MapOperation implements ReadonlyOperation {

    private Query query;

    public HDQueryOperation() {
    }

    public HDQueryOperation(Query query) {
        super(query.getMapName());
        this.query = query;
    }

    @Override
    public void run() {
        QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(getName());
        runAsyncPartitionThreadScanForNative(queryRunner);
    }

    private void runAsyncPartitionThreadScanForNative(QueryRunner queryRunner) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        ops.onStartAsyncOperation(this);
        runPartitionScanOnPartitionThreadsAsyncBatched(query, queryRunner);
    }

    void runPartitionScanOnPartitionThreadsAsyncBatched(final Query query, final QueryRunner queryRunner) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        PartitionAwareCallableBatchingRunnable queryRunnable = new PartitionAwareCallableBatchingRunnable(
                getNodeEngine(), new QueryPartitionCallableFactory(query, mapServiceContext));

        ops.getOperationExecutor().executeOnPartitionThreads(queryRunnable);
        queryRunnable.getFuture().andThen(
                new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        try {
                            Result modifiableResult;
                            try {
                                modifiableResult = queryRunner.populateEmptyResult(query, Collections.<Integer>emptyList());
                                populateResult((List) response, modifiableResult);
                            } catch (Exception e) {
                                HDQueryOperation.this.sendResponse(e);
                                throw rethrow(e);
                            }
                            HDQueryOperation.this.sendResponse(modifiableResult);
                        } finally {
                            ops.onCompletionAsyncOperation(HDQueryOperation.this);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            HDQueryOperation.this.sendResponse(t);
                        } finally {
                            ops.onCompletionAsyncOperation(HDQueryOperation.this);
                        }
                    }
                });
    }

    private static class QueryPartitionCallableFactory implements PartitionAwareCallableFactory {
        private final Query query;
        private final MapServiceContext mapServiceContext;

        public QueryPartitionCallableFactory(Query query, MapServiceContext mapServiceContext) {
            this.query = query;
            this.mapServiceContext = mapServiceContext;
        }

        @Override
        public PartitionAwareCallable create() {
            return new QueryPartitionCallable(query, mapServiceContext);
        }
    }

    private static class QueryPartitionCallable implements PartitionAwareCallable {
        private final MapServiceContext mapServiceContext;
        private final Query query;

        public QueryPartitionCallable(Query query, MapServiceContext mapServiceContext) {
            this.query = query;
            this.mapServiceContext = mapServiceContext;
        }

        @Override
        public Object call(int partitionId) {
            QueryRunner queryRunner = mapServiceContext.getMapQueryRunner(query.getMapName());
            return queryRunner.runPartitionIndexOrPartitionScanQueryOnGivenOwnedPartition(query, partitionId);
        }
    }

    private Result populateResult(List resultObjects, Result result) {
        if (resultObjects == null) {
            return result;
        }
        for (Object resultObject : resultObjects) {
            if (resultObject instanceof Result) {
                Result partitionResult = (Result) resultObject;
                result.combine(partitionResult);
            }
        }
        return result;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        // This is required since if the returnsResponse() method returns false there won't be any response sent
        // to the invoking party - this means that the operation won't be retried if the exception is instanceof
        // HazelcastRetryableException
        sendResponse(e);
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

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.QUERY_OP;
    }

    @Override
    public final int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }
}
