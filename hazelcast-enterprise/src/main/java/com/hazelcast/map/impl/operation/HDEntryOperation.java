package com.hazelcast.map.impl.operation;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.core.Offloadable.NO_OFFLOADING;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;
import static com.hazelcast.spi.ExecutionService.OFFLOADABLE_EXECUTOR;

/**
 * Contains HD Version of the Offloadable contract for EntryProcessor.
 * See the javadoc on {@link EntryOperation} for full documentation.
 *
 * GOTCHA : This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
@SuppressWarnings("checkstyle:methodcount")
public class HDEntryOperation extends HDKeyBasedMapOperation implements BackupAwareOperation, MutatingOperation,
        BlockingOperation {

    private EntryProcessor entryProcessor;

    private transient boolean offloading;

    // HDEntryOperation
    private transient Object oldValue;
    private transient EntryEventType eventType;
    private transient Object response;
    private transient Object dataValue;

    // HDEntryOffloadableOperation
    private transient boolean readOnly;
    private transient long begin;
    private transient OperationServiceImpl ops;

    public HDEntryOperation() {
    }

    public HDEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (entryProcessor instanceof Offloadable) {
            String executorName = ((Offloadable) entryProcessor).getExecutorName();
            if (!executorName.equals(NO_OFFLOADING)) {
                offloading = true;
            }
        }

        if (offloading) {
            this.ops = (OperationServiceImpl) getNodeEngine().getOperationService();
            this.begin = getNow();
            this.readOnly = entryProcessor instanceof ReadOnly;
        }

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        if (offloading) {
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

        Object value = recordStore.get(dataKey, false);
        value = new HeapData(((Data) value).toByteArray());

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
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                try {
                    final Map.Entry entry = createMapEntry(dataKey, previousValue);
                    final Data result = process(entry);
                    if (!noOp(entry, previousValue)) {
                        throwModificationInReadOnlyException();
                    }
                    getOperationResponseHandler().sendResponse(HDEntryOperation.this, result);
                } catch (Throwable t) {
                    getOperationResponseHandler().sendResponse(HDEntryOperation.this, t);
                } finally {
                    ops.onCompletionAsyncOperation(HDEntryOperation.this);
                }
            }
        });
    }

    private void throwModificationInReadOnlyException() {
        throw new UnsupportedOperationException("Entry Processor " + entryProcessor.getClass().getName()
                + " marked as ReadOnly tried to modify map " + name + ". This is not supported. Remove "
                + "the ReadOnly marker from the Entry Processor or do not modify the entry in the process "
                + "method.");
    }

    @SuppressWarnings("unchecked")
    private void runOffloadedModifyingEntryProcessor(final Object previousValue, String executorName) {
        final OperationServiceImpl ops = (OperationServiceImpl) getNodeEngine().getOperationService();

        final Data finalDataKey = dataKey;
        final String finalCaller = getCallerUuid();
        final long finalThreadId = threadId;
        final long finalCallId = getCallId();
        final long finalBegin = begin;

        // The off-loading uses local locks, since the locking is used only on primary-replica.
        // The locks are not supposed to be migrated on partition migration or partition promotion & downgrade.
        recordStore.localLock(finalDataKey, finalCaller, finalThreadId, finalCallId, -1);

        ops.onStartAsyncOperation(this);
        getNodeEngine().getExecutionService().execute(executorName, new Runnable() {
            @Override
            public void run() {
                try {
                    final Map.Entry entry = createMapEntry(dataKey, previousValue);
                    final Data result = process(entry);
                    if (!noOp(entry, previousValue)) {
                        Data newValue = toData(entry.getValue());

                        EntryEventType modificationType;
                        if (entry.getValue() == null) {
                            modificationType = REMOVED;
                        } else {
                            modificationType = (previousValue == null) ? ADDED : UPDATED;
                        }

                        updateAndUnlock(toData(previousValue), newValue, modificationType, finalCaller, finalThreadId,
                                result, finalBegin);
                    } else {
                        unlockOnly(result, finalCaller, finalThreadId, finalBegin);
                    }
                } catch (Throwable t) {
                    unlockOnly(t, finalCaller, finalThreadId, finalBegin);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void updateAndUnlock(Data previousValue, Data newValue, EntryEventType modificationType, String caller,
                                 long threadId, final Object result, long now) {
        HDEntryOffloadableSetUnlockOperation updateOperation = new HDEntryOffloadableSetUnlockOperation(name, modificationType,
                dataKey, previousValue, newValue, caller, threadId, now, entryProcessor.getBackupProcessor());

        updateOperation.setPartitionId(getPartitionId());
        ops.invokeOnPartition(updateOperation).andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                try {
                    getOperationResponseHandler().sendResponse(HDEntryOperation.this, result);
                } finally {
                    ops.onCompletionAsyncOperation(HDEntryOperation.this);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    // EntryOffloadableLockMismatchException is a marker send from the EntryOffloadableSetUnlockOperation
                    // meaning that the whole invocation of the EntryOperation should be retried
                    if (t instanceof EntryOffloadableLockMismatchException) {
                        t = new RetryableHazelcastException(t.getMessage(), t);
                    }
                    getOperationResponseHandler().sendResponse(HDEntryOperation.this, t);
                } finally {
                    ops.onCompletionAsyncOperation(HDEntryOperation.this);
                }
            }
        });
    }

    private void unlockOnly(final Object result, String caller, long threadId, long now) {
        updateAndUnlock(null, null, null, caller, threadId, result, now);
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (offloading) {
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
        if (offloading) {
            // This has to be false, since the operation uses the deferred-response mechanism.
            // This method returns false, but the response will be send later on using the response handler
            return false;
        } else {
            return super.returnsResponse();
        }
    }

    private void runVanilla() {
        final long now = getNow();
        oldValue = recordStore.get(dataKey, false);

        Map.Entry entry = createMapEntry(dataKey, oldValue);

        response = process(entry);

        // first call noOp, other if checks below depends on it.
        if (noOp(entry, oldValue)) {
            return;
        }

        // at this stage we see that the entry has been modified which is not allowed for readOnly processors
        if (entryProcessor instanceof ReadOnly) {
            throwModificationInReadOnlyException();
        }

        if (entryRemoved(entry, now)) {
            return;
        }
        entryAddedOrUpdated(entry, now);
    }

    @Override
    public void afterRun() throws Exception {
        if (!offloading) {
            super.afterRun();
            if (eventType == null) {
                return;
            }
            mapServiceContext.interceptAfterPut(name, dataValue);
            if (isPostProcessing(recordStore)) {
                Record record = recordStore.getRecord(dataKey);
                dataValue = record == null ? null : record.getValue();
            }

            invalidateNearCache(dataKey);
            publishEntryEvent();
            publishWanReplicationEvent();
            evict(dataKey);

            disposeDeferredBlocks();
        }
    }

    @Override
    public final WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        // optimisation for ReadOnly processors -> they will not wait for the lock
        if (entryProcessor instanceof ReadOnly) {
            return false;
        }
        return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        if (offloading) {
            return null;
        }
        return response;
    }

    @Override
    public Operation getBackupOperation() {
        if (offloading) {
            return null;
        }
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new HDEntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        if (offloading) {
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

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    private Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    private boolean entryRemoved(Map.Entry entry, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.delete(dataKey);
            getLocalMapStats().incrementRemoves(getLatencyFrom(now));
            eventType = REMOVED;
            return true;
        }
        return false;
    }

    /**
     * Only difference between add and update is the event type to be published.
     */
    private void entryAddedOrUpdated(Map.Entry entry, long now) {
        dataValue = entry.getValue();
        recordStore.set(dataKey, dataValue, DEFAULT_TTL);
        getLocalMapStats().incrementPuts(getLatencyFrom(now));

        eventType = oldValue == null ? ADDED : UPDATED;
    }

    private Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    private boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
    }

    /**
     * Nullify old value if in-memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in-memory format.
     */
    private void nullifyOldValueIfNecessary() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && eventType != REMOVED) {
            oldValue = null;
        }
    }

    private void publishEntryEvent() {
        if (hasRegisteredListenerForThisMap()) {
            nullifyOldValueIfNecessary();
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);
        }
    }

    private void publishWanReplicationEvent() {
        final MapContainer mapContainer = this.mapContainer;
        if (mapContainer.getWanReplicationPublisher() == null
                && mapContainer.getWanMergePolicy() == null) {
            return;
        }
        final Data key = dataKey;

        if (REMOVED.equals(eventType)) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                dataValue = toData(dataValue);
                final EntryView entryView = createSimpleEntryView(key, dataValue, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
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
}
