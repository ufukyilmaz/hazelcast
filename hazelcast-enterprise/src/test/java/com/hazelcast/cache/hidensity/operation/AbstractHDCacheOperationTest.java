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
import com.hazelcast.map.impl.operation.HDPutAllBackupOperation;
import com.hazelcast.map.impl.operation.HDPutAllOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.OngoingStubbing;

import javax.cache.expiry.ExpiryPolicy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.PUT;
import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.PUT_ALL;
import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.SET_EXPIRY_POLICY;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
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

    @Before
    public void setUp() {
        EnterpriseSerializationService serializationService = mock(EnterpriseSerializationService.class);
        when(serializationService.toData(any())).thenReturn(new HeapData());

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(Logger.getLogger(getClass()));
        when(nodeEngine.getSerializationService()).thenReturn(serializationService);

        cacheConfig = new CacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        CacheRecord record = mock(CacheRecord.class);
        recordProcessor = mock(HiDensityRecordProcessor.class);

        recordStore = mock(HiDensityCacheRecordStore.class);
        when(recordStore.getConfig()).thenReturn(cacheConfig);
        when(recordStore.putBackup(any(Data.class), any(), any(ExpiryPolicy.class))).thenReturn(record);
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
                    putStub.thenReturn(null, null, null)
                            .thenThrow(exception, exception, exception, exception, exception, exception)
                            .thenReturn(null);
                    numberOfNativeOOME = 6;
                } else {
                    putStub.thenReturn(null);
                    numberOfNativeOOME = 0;
                }
                return;
            case PUT:
                OngoingStubbing<Object> getAndPutStub = when(recordStore.getAndPut(any(Data.class), any(Data.class),
                        any(ExpiryPolicy.class), anyString(), anyInt()));
                if (throwNativeOOME) {
                    getAndPutStub.thenReturn(null, null, null)
                            .thenThrow(exception, exception, exception, exception, exception, exception)
                            .thenReturn(null);
                    numberOfNativeOOME = 6;
                } else {
                    getAndPutStub.thenReturn(null);
                    numberOfNativeOOME = 0;
                }
                return;
            case SET_EXPIRY_POLICY:
                if (throwNativeOOME) {
                    doNothing().doNothing().doNothing()
                            .doThrow(exception).doThrow(exception)
                            .doThrow(exception).doThrow(exception)
                            .doThrow(exception).doThrow(exception)
                            .doNothing().when(recordStore).setExpiryPolicy(ArgumentMatchers.<Data>anyCollection(), any(), anyString());
                    numberOfNativeOOME = 6;
                } else {
                    doNothing().when(recordStore).setExpiryPolicy(ArgumentMatchers.<Data>anyCollection(), any(), anyString());
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

        if (operationType == PUT_ALL) {
            // RecordStore.put() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).put(any(Data.class), any(Data.class),
                    any(ExpiryPolicy.class), anyString(), anyInt());

            verify(recordStore, atLeastOnce()).getConfig();
        } else if (operationType == PUT) {
            // RecordStore.getAndPut() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).getAndPut(any(Data.class), any(Data.class),
                    any(ExpiryPolicy.class), anyString(), anyInt());
        } else if (operationType == SET_EXPIRY_POLICY) {
            if (verifyBackups) {
                verify(recordStore, times((syncBackupCount + 1) * ENTRY_COUNT + numberOfNativeOOME))
                        .setExpiryPolicy(any(Collection.class), any(), anyString());
            } else {
                verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME))
                        .setExpiryPolicy(any(Collection.class), any(), anyString());
            }
        }

        if (verifyBackups) {
            verify(recordStore, times(ENTRY_COUNT)).putBackup(nullable(Data.class), nullable(Data.class), any(ExpiryPolicy.class));
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
