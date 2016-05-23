package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.logging.ILogger;
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
import com.hazelcast.spi.Operation;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import java.util.List;

import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

abstract class AbstractHDOperationTest {

    static final ILogger LOGGER = getLogger(AbstractHDOperationTest.class);

    int syncBackupCount;
    boolean throwNativeOOME;

    private MapService mapService;
    private MapContainer mapContainer;

    private RecordStore recordStore;
    private HDEvictorImpl hdEvictor;
    private NearCacheInvalidator nearCacheInvalidator;

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

        PartitionContainer partitionContainer = mock(PartitionContainer.class);
        when(partitionContainer.getRecordStore(eq(getMapName()))).thenReturn(recordStore);
        when(partitionContainer.getExistingRecordStore(eq(getMapName()))).thenReturn(recordStore);

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
    void configureRecordStore() {
        OngoingStubbing<Boolean> stub = when(recordStore.set(any(Data.class), any(), anyLong()));
        if (throwNativeOOME) {
            stub.thenReturn(true, true, true).thenThrow(new NativeOutOfMemoryError()).thenReturn(true);
        } else {
            stub.thenReturn(true);
        }
    }

    /**
     * Asserts the backup configuration of the {@link HDPutAllOperation}.
     *
     * @param operation the {@link HDPutAllOperation} to check
     */
    void assertBackupConfiguration(HDPutAllOperation operation) {
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
        operation.setMapService(mapService);
        operation.setService(mapService);
        if (partitionId != 0) {
            operation.setPartitionId(partitionId);
        }

        operation.beforeRun();
        operation.run();
        operation.afterRun();
    }

    /**
     * Verifies the {@link RecordStore} mock after a call of {@link HDPutAllOperation#run()},
     * {@link HDPutAllBackupOperation#run()} or {@link HDPutAllPerMemberOperation#run()}.
     *
     * @param isBackupDone {@code true} if backup operation has been called, {@code false} otherwise
     */
    void verifyRecordStoreAfterOperation(boolean isBackupDone) {
        boolean verifyBackups = syncBackupCount > 0 && isBackupDone;
        int numberOfExceptions = (throwNativeOOME ? 1 : 0);
        int backupFactor = (verifyBackups ? 2 : 1);
        int partitionCount = getPartitionCount();
        int itemCount = getItemCount();

        // RecordStore.getMapContainer() is called again for each NativeOOME during the forced eviction
        verify(recordStore, times(partitionCount * backupFactor + numberOfExceptions)).getMapContainer();
        // RecordStore.set() is called again for each entry which threw a NativeOOME
        verify(recordStore, times(partitionCount * itemCount + numberOfExceptions)).set(any(Data.class), any(), anyLong());

        verify(recordStore, times(partitionCount * itemCount * syncBackupCount)).getRecord(any(Data.class));
        verify(recordStore, times(partitionCount * itemCount * backupFactor)).evictEntries();
        verify(recordStore, times(partitionCount * itemCount)).getMapDataStore();

        if (verifyBackups) {
            verify(recordStore, times(partitionCount * itemCount)).putBackup(any(Data.class), any());
        }

        verifyNoMoreInteractions(recordStore);
    }

    /**
     * Verifies the {@link NearCacheInvalidator} mock after a {@link HDPutAllOperation#afterRun()} call.
     */
    @SuppressWarnings("unchecked")
    void verifyNearCacheInvalidatorAfterOperation() {
        ArgumentCaptor<List<Data>> keysCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(nearCacheInvalidator, times(getPartitionCount())).invalidate(eq(getMapName()), keysCaptor.capture(), anyString());
        verifyNoMoreInteractions(nearCacheInvalidator);

        List<Data> keys = keysCaptor.getAllValues().get(0);
        assertEquals("NearCacheInvalidator should have received all keys", getItemCount(), keys.size());
    }

    /**
     * Verifies that there is a single forced eviction if a NativeOOME has been thrown
     */
    void verifyHDEvictor() {
        if (throwNativeOOME) {
            verify(hdEvictor).forceEvict(eq(recordStore));
            verifyNoMoreInteractions(hdEvictor);
        } else {
            verifyZeroInteractions(hdEvictor);
        }
    }

    abstract String getMapName();

    abstract int getItemCount();

    abstract int getPartitionCount();
}
