package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.nearcache.NearCacheInvalidator;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.map.impl.operation.AbstractHDOperationTest.OperationType.PUT;
import static com.hazelcast.map.impl.operation.AbstractHDOperationTest.OperationType.PUT_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

abstract class AbstractHDOperationTest {

    enum OperationType {
        PUT_ALL,
        PUT
    }

    int syncBackupCount;
    boolean throwNativeOOME;

    private int numberOfNativeOOME;

    private MapService mapService;
    private MapContainer mapContainer;

    private RecordStore recordStore;
    private HDEvictorImpl hdEvictor;
    private NearCacheInvalidator nearCacheInvalidator;

    private NodeEngine nodeEngine;

    public void setUp() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        hdEvictor = mock(HDEvictorImpl.class);

        mapContainer = mock(MapContainer.class);
        when(mapContainer.getWanReplicationPublisher()).thenReturn(null);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        when(mapContainer.getEvictor()).thenReturn(hdEvictor);
        when(mapContainer.isInvalidationEnabled()).thenReturn(true);

        MapDataStore mapDataStore = mock(MapDataStore.class);

        Record record = mock(Record.class);

        recordStore = mock(RecordStore.class);
        when(recordStore.getMapDataStore()).thenReturn(mapDataStore);
        when(recordStore.getMapContainer()).thenReturn(mapContainer);
        when(recordStore.getRecord(any(Data.class))).thenReturn(record);
        when(recordStore.putBackup(any(Data.class), any())).thenReturn(record);
        when(recordStore.putBackup(any(Data.class), any(Data.class), anyInt(), anyBoolean())).thenReturn(record);

        PartitionContainer partitionContainer = mock(PartitionContainer.class);
        when(partitionContainer.getRecordStore(eq(getMapName()))).thenReturn(recordStore);
        when(partitionContainer.getExistingRecordStore(eq(getMapName()))).thenReturn(recordStore);
        when(partitionContainer.getMaps()).thenReturn(new ConcurrentHashMap<String, RecordStore>());

        MapEventPublisher mapEventPublisher = mock(MapEventPublisher.class);
        when(mapEventPublisher.hasEventListener(eq(getMapName()))).thenReturn(false);

        nearCacheInvalidator = mock(NearCacheInvalidator.class);

        NearCacheProvider nearCacheProvider = mock(NearCacheProvider.class, RETURNS_DEEP_STUBS);
        when(nearCacheProvider.getNearCacheInvalidator()).thenReturn(nearCacheInvalidator);

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getPartitionContainer(anyInt())).thenReturn(partitionContainer);
        when(mapServiceContext.getMapEventPublisher()).thenReturn(mapEventPublisher);
        when(mapServiceContext.getNearCacheProvider()).thenReturn(nearCacheProvider);
        when(mapServiceContext.getMapContainer(eq(getMapName()))).thenReturn(mapContainer);

        mapService = mock(MapService.class);
        when(mapService.getMapServiceContext()).thenReturn(mapServiceContext);

        IPartitionService partitionService = mock(IPartitionService.class);
        when(partitionService.getPartitionCount()).thenReturn(getPartitionCount());

        InternalOperationService operationService = mock(InternalOperationService.class);
        when(operationService.getPartitionThreadCount()).thenReturn(getPartitionCount());

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(Logger.getLogger(getClass()));
        when(nodeEngine.getPartitionService()).thenReturn(partitionService);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
    }

    /**
     * Creates a {@link MapEntries} instances with mocked {@link Data} entries.
     *
     * @param itemCount number of entries
     * @return a {@link MapEntries} instance
     */
    MapEntries createMapEntries(int itemCount) {
        MapEntries mapEntries = new MapEntries(itemCount);
        for (int i = 0; i < itemCount; i++) {
            Data key = mock(Data.class);
            Data value = mock(Data.class);
            mapEntries.add(key, value);
        }
        return mapEntries;
    }

    /**
     * Configures the number of sync backups.
     */
    void configureBackups() {
        when(mapContainer.getBackupCount()).thenReturn(syncBackupCount);
        when(mapContainer.getTotalBackupCount()).thenReturn(syncBackupCount);
    }

    /**
     * Configures the {@link RecordStore} mock.
     */
    void configureRecordStore(OperationType operationType) {
        NativeOutOfMemoryError exception = new NativeOutOfMemoryError();
        switch (operationType) {
            case PUT_ALL:
                OngoingStubbing<Boolean> setStub = when(recordStore.set(any(Data.class), any(), anyLong()));
                if (throwNativeOOME) {
                    setStub.thenReturn(true, true, true).thenThrow(exception).thenReturn(true);
                    numberOfNativeOOME = 1;
                } else {
                    setStub.thenReturn(true);
                    numberOfNativeOOME = 0;
                }
                return;

            case PUT:
                OngoingStubbing<Object> putStub = when(recordStore.put(any(Data.class), any(Data.class), anyLong()));
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
     * @throws Exception if {@link MapOperation#beforeRun()} or {@link MapOperation#afterRun()} throws an exception
     */
    void executeMapOperation(Operation operation, int partitionId) throws Exception {
        executeMapOperation((MapOperation) operation, partitionId);
    }

    /**
     * Executes a {@link MapOperation} with the necessary mocks and settings.
     *
     * @throws Exception if {@link MapOperation#beforeRun()} or {@link MapOperation#afterRun()} throws an exception
     */
    void executeMapOperation(MapOperation operation, int partitionId) throws Exception {
        prepareOperation(operation);
        if (partitionId != 0) {
            operation.setPartitionId(partitionId);
        }

        operation.beforeRun();
        operation.run();
        operation.afterRun();
    }

    void prepareOperation(MapOperation operation) {
        operation.setMapService(mapService);
        operation.setService(mapService);
        operation.setNodeEngine(nodeEngine);
    }

    /**
     * Verifies the {@link RecordStore} mock after a call of {@link HDPutAllOperation#run()} or
     * {@link HDPutAllBackupOperation#run()}.
     *
     * @param isBackupDone {@code true} if backup operation has been called, {@code false} otherwise
     */
    void verifyRecordStoreAfterOperation(OperationType operationType, boolean isBackupDone) {
        boolean verifyBackups = syncBackupCount > 0 && isBackupDone;
        int backupFactor = (verifyBackups ? 2 : 1);
        int itemCount = getItemCount();

        if (operationType == PUT_ALL) {
            // RecordStore.set() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(itemCount + numberOfNativeOOME)).set(any(Data.class), any(), anyLong());
        } else if (operationType == PUT) {
            // RecordStore.put() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(itemCount + numberOfNativeOOME)).put(any(Data.class), any(), anyLong());
        }

        verify(recordStore, times(itemCount * syncBackupCount)).getRecord(any(Data.class));
        verify(recordStore, times(itemCount * backupFactor)).evictEntries(any(Data.class));
        verify(recordStore, atLeast(itemCount)).getMapDataStore();
        verify(recordStore, atLeastOnce()).getMapContainer();

        if (verifyBackups) {
            if (operationType == PUT_ALL) {
                verify(recordStore, times(itemCount)).putBackup(any(Data.class), any());
            } else {
                verify(recordStore, times(itemCount)).putBackup(any(Data.class), any(Data.class), anyInt(), anyBoolean());
            }
        }

        verifyNoMoreInteractions(recordStore);
    }

    /**
     * Verifies the {@link NearCacheInvalidator} mock after a {@link HDPutAllOperation#afterRun()} call.
     */
    @SuppressWarnings("unchecked")
    void verifyNearCacheInvalidatorAfterOperation() {
        ArgumentCaptor<List<Data>> keysCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(nearCacheInvalidator, times(1)).invalidate(eq(getMapName()), keysCaptor.capture(), anyString());
        verifyNoMoreInteractions(nearCacheInvalidator);

        List<Data> keys = keysCaptor.getAllValues().get(0);
        assertEquals("NearCacheInvalidator should have received all keys", getItemCount(), keys.size());
    }

    /**
     * Verifies that there is a single forced eviction if a NativeOOME has been thrown
     */
    void verifyHDEvictor(OperationType operationType) {
        if (throwNativeOOME) {
            int expectedCount = (operationType == PUT) ? getItemCount() : 1;
            verify(hdEvictor, times(expectedCount)).forceEvict(eq(recordStore));
            verifyNoMoreInteractions(hdEvictor);
        } else {
            verifyZeroInteractions(hdEvictor);
        }
    }

    abstract String getMapName();

    abstract int getItemCount();

    abstract int getPartitionCount();
}
