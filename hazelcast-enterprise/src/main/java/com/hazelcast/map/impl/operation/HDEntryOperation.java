package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.core.Offloadable.NO_OFFLOADING;
import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.spi.ExecutionService.OFFLOADABLE_EXECUTOR;
import static com.hazelcast.spi.InvocationBuilder.DEFAULT_TRY_PAUSE_MILLIS;
import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

/**
 * Contains HD Version of the Offloadable contract for EntryProcessor.
 * See the javadoc on {@link EntryOperation} for full documentation.
 * <p>
 * GOTCHA : This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDEntryOperation extends HDKeyBasedMapOperation
        implements BackupAwareOperation, MutatingOperation, BlockingOperation {

    private static final int SET_UNLOCK_FAST_RETRY_LIMIT = 10;

    private EntryProcessor entryProcessor;

    private transient boolean offload;

    // HDEntryOperation
    private transient Object response;

    // HDEntryOffloadableOperation
    private transient boolean readOnly;
    private transient long begin;
    private transient OperationServiceImpl ops;
    private transient ExecutionService exs;

    private transient int setUnlockRetryCount;

    public HDEntryOperation() {
    }

    public HDEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        this.ops = (OperationServiceImpl) getNodeEngine().getOperationService();
        this.exs = getNodeEngine().getExecutionService();
        this.begin = getNow();
        this.readOnly = entryProcessor instanceof ReadOnly;

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        if (offload) {
            runOffloaded();
        } else {
            runVanilla();
        }
    }

    private void runOffloaded() {
        if (!(entryProcessor instanceof Offloadable)) {
            throw new HazelcastException("EntryProcessor is expected to implement Offloadable for this operation");
        }
        if (readOnly && entryProcessor.getBackupProcessor() != null) {
            throw new HazelcastException("EntryProcessor.getBackupProcessor() should return null if ReadOnly implemented");
        }

        Object value = recordStore.get(dataKey, false, getCallerAddress());
        value = value == null ? null : toHeapData((Data) value);

        String executorName = ((Offloadable) entryProcessor).getExecutorName();
        executorName = executorName.equals(Offloadable.OFFLOADABLE_EXECUTOR) ? OFFLOADABLE_EXECUTOR : executorName;

        if (readOnly) {
            runOffloadedReadOnlyEntryProcessor(value, executorName);
        } else {
            runOffloadedModifyingEntryProcessor(value, executorName);
        }
    }

    @SuppressWarnings("unchecked")
    private void runOffloadedReadOnlyEntryProcessor(final Object previousValue, String executorName) {
        ops.onStartAsyncOperation(this);
        exs.execute(executorName, () -> {
            try {
                Data result = operator(HDEntryOperation.this, entryProcessor)
                        .operateOnKeyValue(dataKey, previousValue).getResult();
                getOperationResponseHandler().sendResponse(HDEntryOperation.this, result);
            } catch (Throwable t) {
                getOperationResponseHandler().sendResponse(HDEntryOperation.this, t);
            } finally {
                ops.onCompletionAsyncOperation(HDEntryOperation.this);
            }
        });
    }

    private void runOffloadedModifyingEntryProcessor(final Object previousValue, String executorName) {
        // callerId is random since the local locks are NOT re-entrant
        // using a randomID every time prevents from re-entering the already acquired lock
        final String finalCaller = UuidUtil.newUnsecureUuidString();
        final Data finalDataKey = dataKey;
        final long finalThreadId = threadId;
        final long finalCallId = getCallId();
        final long finalBegin = begin;

        // The off-loading uses local locks, since the locking is used only on primary-replica.
        // The locks are not supposed to be migrated on partition migration or partition promotion & downgrade.
        lock(finalDataKey, finalCaller, finalThreadId, finalCallId);

        try {
            ops.onStartAsyncOperation(this);
            exs.execute(executorName, () -> {
                try {
                    EntryOperator operator = EntryOperator.operator(HDEntryOperation.this, entryProcessor);
                    Data result = operator.operateOnKeyValue(dataKey, previousValue).getResult();
                    EntryEventType modificationType = operator.getEventType();
                    if (modificationType != null) {
                        Data newValue = toData(operator.getNewValue());
                        updateAndUnlock(toData(previousValue), newValue, modificationType, finalCaller, finalThreadId,
                                result, finalBegin);
                    } else {
                        unlockOnly(result, finalCaller, finalThreadId, finalBegin);
                    }
                } catch (Throwable t) {
                    unlockOnly(t, finalCaller, finalThreadId, finalBegin);
                }
            });
        } catch (Throwable t) {
            try {
                unlock(finalDataKey, finalCaller, finalThreadId, finalCallId, t);
                sneakyThrow(t);
            } finally {
                ops.onCompletionAsyncOperation(this);
            }
        }
    }

    private void lock(Data finalDataKey, String finalCaller, long finalThreadId, long finalCallId) {
        boolean locked = recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);
        if (!locked) {
            // should not happen since it's a lock-awaiting operation and we are on a partition-thread, but just to make sure
            throw new IllegalStateException(
                    String.format("Could not obtain a lock by the caller=%s and threadId=%d", finalCaller, threadId));
        }
    }

    private void unlock(Data finalDataKey, String finalCaller, long finalThreadId, long finalCallId, Throwable cause) {
        boolean unlocked = recordStore.unlock(finalDataKey, finalCaller, finalThreadId, finalCallId);
        if (!unlocked) {
            throw new IllegalStateException(
                    String.format("Could not unlock by the caller=%s and threadId=%d", finalCaller, threadId), cause);
        }
    }

    @SuppressWarnings({"checkstyle:methodlength"})
    private void updateAndUnlock(Data previousValue, Data newValue, EntryEventType modificationType, String caller,
                                 long threadId, final Object result, long now) {
        Operation setUnlockOperation = new HDEntryOffloadableSetUnlockOperation(name, modificationType,
                dataKey, previousValue, newValue, caller, threadId, now, entryProcessor.getBackupProcessor())
                .setPartitionId(getPartitionId())
                .setReplicaIndex(0)
                .setNodeEngine(getNodeEngine())
                .setCallerUuid(getCallerUuid())
                .setOperationResponseHandler(new SetUnlockOperationResponseHandler(result));

        OperationAccessor.setCallerAddress(setUnlockOperation, getCallerAddress());
        ops.execute(setUnlockOperation);
    }

    private boolean isRetryable(Object response) {
        return response instanceof RetryableHazelcastException && !(response instanceof WrongTargetException);
    }

    private boolean isTimeout(Object response) {
        return response instanceof CallTimeoutResponse;
    }

    private boolean isFastRetryLimitReached() {
        return setUnlockRetryCount > SET_UNLOCK_FAST_RETRY_LIMIT;
    }

    private void unlockOnly(final Object result, String caller, long threadId, long now) {
        updateAndUnlock(null, null, null, caller, threadId, result, now);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (offload) {
            // This is required since if the returnsResponse() method returns false there won't be any response sent
            // to the invoking party - this means that the operation won't be retried if the exception is instanceof
            // HazelcastRetryableException
            sendResponse(e);
        } else {
            super.onExecutionFailure(e);
        }
    }

    @Override
    public boolean returnsResponse() {
        if (offload) {
            // This has to be false, since the operation uses the deferred-response mechanism.
            // This method returns false, but the response will be send later on using the response handler
            return false;
        } else {
            return super.returnsResponse();
        }
    }

    private void runVanilla() {
        response = operator(this, entryProcessor)
                .operateOnKey(dataKey).doPostOperateOps().getResult();
    }

    @Override
    public void afterRun() throws Exception {
        if (!offload) {
            disposeDeferredBlocks();
        }
    }

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        // optimisation for ReadOnly processors -> they will not wait for the lock
        if (entryProcessor instanceof ReadOnly) {
            offload = isOffloadingRequested(entryProcessor);
            return false;
        }
        // mutating offloading -> only if key not locked, since it uses locking too (but on reentrant one)
        if (!recordStore.isLocked(dataKey) && isOffloadingRequested(entryProcessor)) {
            offload = true;
            return false;
        }
        //at this point we cannot offload. the entry is locked or the EP does not support offloading
        //if the entry is locked by us then we can still run the EP on the partition thread
        offload = false;
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    private boolean isOffloadingRequested(EntryProcessor entryProcessor) {
        if (entryProcessor instanceof Offloadable) {
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            if (!executorName.equals(NO_OFFLOADING)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        if (offload) {
            return null;
        }
        return response;
    }

    @Override
    public Operation getBackupOperation() {
        if (offload) {
            return null;
        }
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new HDEntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        if (offload) {
            return false;
        }
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    private Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.ENTRY;
    }

    private class SetUnlockOperationResponseHandler implements OperationResponseHandler {
        private final Object result;

        SetUnlockOperationResponseHandler(Object result) {
            this.result = result;
        }

        @Override
        public void sendResponse(Operation op, Object response) {
            if (isRetryable(response) || isTimeout(response)) {
                retry(op);
            } else {
                handleResponse(response);
            }
        }

        private void retry(final Operation op) {
            setUnlockRetryCount++;
            if (isFastRetryLimitReached()) {
                exs.schedule(() -> ops.execute(op), DEFAULT_TRY_PAUSE_MILLIS, TimeUnit.MILLISECONDS);
            } else {
                ops.execute(op);
            }
        }

        private void handleResponse(Object response) {
            if (response instanceof Throwable) {
                Throwable t = (Throwable) response;
                try {
                    // EntryOffloadableLockMismatchException is a marker send from the EntryOffloadableSetUnlockOperation
                    // meaning that the whole invocation of the EntryOffloadableOperation should be retried
                    if (t instanceof EntryOffloadableLockMismatchException) {
                        t = new RetryableHazelcastException(t.getMessage(), t);
                    }
                    HDEntryOperation.this.sendResponse(t);
                } finally {
                    ops.onCompletionAsyncOperation(HDEntryOperation.this);
                }
            } else {
                try {
                    HDEntryOperation.this.sendResponse(result);
                } finally {
                    ops.onCompletionAsyncOperation(HDEntryOperation.this);
                }
            }
        }
    }
}
