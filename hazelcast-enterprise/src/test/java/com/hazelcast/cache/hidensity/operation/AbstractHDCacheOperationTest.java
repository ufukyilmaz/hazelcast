package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.hidensity.HiDensityCacheRecordStore;
import com.hazelcast.cache.impl.EnterpriseCacheService;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.hidensity.HiDensityRecordProcessor;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public abstract class AbstractHDCacheOperationTest {

    static final int ENTRY_COUNT = 5;
    static final int PARTITION_ID = 23;

    enum OperationType {
        PUT,
        PUT_ALL,
        SET_EXPIRY_POLICY
    }

    int syncBackupCount;
    boolean throwNativeOOME;

    EnterpriseCacheService cacheService;
    HiDensityCacheRecordStore recordStore;
    HiDensityRecordProcessor recordProcessor;

    private int numberOfNativeOOME;

    private CacheConfig cacheConfig;
    private NodeEngine nodeEngine;
    private CacheRecord record;

    @Before
    public void setUp() {
        EnterpriseSerializationService serializationService = mock(EnterpriseSerializationService.class);
        when(serializationService.toData(any())).thenReturn(new HeapData());

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(Logger.getLogger(getClass()));
        when(nodeEngine.getSerializationService()).thenReturn(serializationService);

        cacheConfig = new CacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        record = mock(CacheRecord.class);
        when(record.getCreationTime()).thenReturn(System.currentTimeMillis());

        recordProcessor = mock(HiDensityRecordProcessor.class);

        recordStore = mock(HiDensityCacheRecordStore.class);
        when(recordStore.getConfig()).thenReturn(cacheConfig);
        when(recordStore.putBackup(any(Data.class), any(), anyLong(), any(ExpiryPolicy.class))).thenReturn(record);
        when(recordStore.getRecord(any(Data.class))).thenReturn(record);
        when(recordStore.getRecordProcessor()).thenReturn(recordProcessor);
        when(recordStore.isWanReplicationEnabled()).thenReturn(false);

        cacheService = mock(EnterpriseCacheService.class);
        when(cacheService.getSerializationService()).thenReturn(serializationService);
        when(cacheService.getRecordStore(eq(getCacheName()), geq(1))).thenReturn(recordStore);
        when(cacheService.getOrCreateRecordStore(eq(getCacheName()), geq(1))).thenReturn(recordStore);
    }

    /**
     * Creates a {@link List<Map.Entry>} instance with mocked {@link Data} entries.
     *
     * @param itemCount number of entries
     * @return a {@link List<Map.Entry>} instance
     */
    List<Map.Entry<Data, Data>> createEntries(int itemCount) {
        List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            Data key = new HeapData(new byte[]{});
            Data value = new HeapData(new byte[]{});
            entries.add(new MapEntrySimple<Data, Data>(key, value));
        }
        return entries;
    }

    /**
     * Configures the number of sync backups.
     */
    void configureBackups() {
        cacheConfig.setBackupCount(syncBackupCount);
    }

    /**
     * Configures the {@link RecordStore} mock.
     */
    void configureRecordStore(OperationType operationType) {
        NativeOutOfMemoryError exception = new NativeOutOfMemoryError();
        switch (operationType) {
            case PUT_ALL:
                OngoingStubbing<CacheRecord> putStub = when(recordStore.put(any(Data.class), any(Data.class),
                        any(ExpiryPolicy.class), anyString(), anyInt()));
                if (throwNativeOOME) {
                    putStub.thenReturn(record, record, record)
                            .thenThrow(exception, exception, exception, exception, exception, exception)
                            .thenReturn(record);
                    numberOfNativeOOME = 6;
                } else {
                    putStub.thenReturn(record);
                    numberOfNativeOOME = 0;
                }
                return;
            case PUT:
                OngoingStubbing<Object> getAndPutStub = when(recordStore.getAndPut(any(Data.class), any(Data.class),
                        any(ExpiryPolicy.class), anyString(), anyInt()));
                if (throwNativeOOME) {
                    getAndPutStub.thenReturn(record, record, record)
                            .thenThrow(exception, exception, exception, exception, exception, exception)
                            .thenReturn(record);
                    numberOfNativeOOME = 6;
                } else {
                    getAndPutStub.thenReturn(record);
                    numberOfNativeOOME = 0;
                }
                return;
            case SET_EXPIRY_POLICY:
                OngoingStubbing<Boolean> expiryPolicyStub = when(recordStore.setExpiryPolicy(ArgumentMatchers.<Data>anyCollection(), any(Data.class), anyString()));
                if (throwNativeOOME) {
                    expiryPolicyStub.thenReturn(true, true, true)
                            .thenThrow(exception, exception, exception, exception, exception, exception)
                            .thenReturn(true);
                    numberOfNativeOOME = 6;
                } else {
                    expiryPolicyStub.thenReturn(true);
                    numberOfNativeOOME = 0;
                }
                return;
            default:
                numberOfNativeOOME = 0;
        }
    }

    /**
     * Asserts the backup configuration of a {@link BackupAwareOperation}.
     *
     * @param operation the {@link BackupAwareOperation} to check
     */
    void assertBackupConfiguration(BackupAwareOperation operation) {
        if (syncBackupCount > 0) {
            assertTrue(operation.shouldBackup());
            assertEquals(syncBackupCount, operation.getSyncBackupCount());
            assertEquals(0, operation.getAsyncBackupCount());
        } else {
            assertFalse(operation.shouldBackup());
            assertEquals(0, operation.getSyncBackupCount());
            assertEquals(0, operation.getAsyncBackupCount());
        }
    }

    /**
     * Executes an {@link Operation} with the necessary mocks and settings.
     *
     * @throws Exception if {@link Operation#beforeRun()} or {@link Operation#afterRun()} throws an exception
     */
    void executeOperation(Operation operation, int partitionId) throws Exception {
        prepareOperation(operation, partitionId);

        operation.beforeRun();
        operation.run();
        operation.afterRun();
    }

    private void prepareOperation(Operation operation, int partitionId) {
        if (operation instanceof HiDensityCacheOperation) {
            HiDensityCacheOperation cacheOperation = (HiDensityCacheOperation) operation;
            cacheOperation.setService(cacheService);
        }
        operation.setNodeEngine(nodeEngine);
        operation.setCallerUuid(newUnsecureUuidString());
        operation.setPartitionId(partitionId);
    }

    /**
     * Verifies the {@link RecordStore} mock after a call of {@link HDPutAllOperation#run()} or
     * {@link HDPutAllBackupOperation#run()}.
     *
     * @param isBackupDone {@code true} if backup operation has been called, {@code false} otherwise
     */
    void verifyRecordStoreAfterRun(OperationType operationType, boolean isBackupDone) {
        boolean verifyBackups = syncBackupCount > 0 && isBackupDone;
        verify(recordStore, atLeastOnce()).getConfig();

        switch (operationType) {
            case PUT_ALL:
                // RecordStore.put() is called again for each entry which threw a NativeOOME
                verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).put(any(Data.class), any(Data.class),
                        any(ExpiryPolicy.class), anyString(), anyInt());
                if (verifyBackups) {
                    verify(recordStore, times(ENTRY_COUNT)).putBackup(nullable(Data.class), nullable(Data.class),
                            anyLong(), any(ExpiryPolicy.class));
                }
                    break;
            case PUT:
                // RecordStore.getAndPut() is called again for each entry which threw a NativeOOME
                verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).getAndPut(any(Data.class), any(Data.class),
                        any(ExpiryPolicy.class), anyString(), anyInt());
                if (syncBackupCount > 0) {
                    verify(recordStore, times(ENTRY_COUNT)).getRecord(any(Data.class));
                }
                if (verifyBackups) {
                    verify(recordStore, times(ENTRY_COUNT)).putBackup(nullable(Data.class), nullable(Data.class),
                            anyLong(), any(ExpiryPolicy.class));
                }
                break;
            case SET_EXPIRY_POLICY:
                if (verifyBackups) {
                    verify(recordStore, times((syncBackupCount + 1) * ENTRY_COUNT + numberOfNativeOOME))
                            .setExpiryPolicy(any(Collection.class), any(), anyString());
                } else {
                    verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME))
                            .setExpiryPolicy(any(Collection.class), any(), anyString());
                }
                break;
        }

        verify(recordStore, atLeastOnce()).isWanReplicationEnabled();
        verify(recordStore, atLeastOnce()).disposeDeferredBlocks();

        verifyNoMoreInteractions(recordStore);
    }

    void verifyForcedEviction() {
        if (!throwNativeOOME) {
            return;
        }

        verify(cacheService, times(ENTRY_COUNT)).forceEvict(eq(getCacheName()), anyInt());
    }

    abstract String getCacheName();
}
