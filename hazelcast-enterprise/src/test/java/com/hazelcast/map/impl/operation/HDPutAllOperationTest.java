package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
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
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;

import java.util.List;

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

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutAllOperationTest {

    private static final int ITEM_COUNT = 5;
    private static final String MAP_NAME = "HDPutAllOperationTest";
    private static final int PARTITION_ID = 23;

    private MapService mapService;
    private MapContainer mapContainer;

    private RecordStore recordStore;
    private HDEvictorImpl hdEvictor;
    private NearCacheInvalidator nearCacheInvalidator;

    private MapEntries mapEntries;

    private int syncBackupCount;
    private boolean throwNativeOOME;

    @Before
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
        when(partitionContainer.getRecordStore(eq(MAP_NAME))).thenReturn(recordStore);
        when(partitionContainer.getExistingRecordStore(eq(MAP_NAME))).thenReturn(recordStore);

        MapEventPublisher mapEventPublisher = mock(MapEventPublisher.class);
        when(mapEventPublisher.hasEventListener(eq(MAP_NAME))).thenReturn(false);

        nearCacheInvalidator = mock(NearCacheInvalidator.class);

        NearCacheProvider nearCacheProvider = mock(NearCacheProvider.class, RETURNS_DEEP_STUBS);
        when(nearCacheProvider.getNearCacheInvalidator()).thenReturn(nearCacheInvalidator);

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getPartitionContainer(anyInt())).thenReturn(partitionContainer);
        when(mapServiceContext.getMapEventPublisher()).thenReturn(mapEventPublisher);
        when(mapServiceContext.getNearCacheProvider()).thenReturn(nearCacheProvider);
        when(mapServiceContext.getMapContainer(eq(MAP_NAME))).thenReturn(mapContainer);

        mapService = mock(MapService.class);
        when(mapService.getMapServiceContext()).thenReturn(mapServiceContext);

        mapEntries = new MapEntries();
        for (int i = 0; i < ITEM_COUNT; i++) {
            Data key = mock(Data.class);
            Data value = mock(Data.class);
            mapEntries.add(key, value);
        }
    }

    @Test
    public void testRun() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = true;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = true;

        testRunInternal();
    }

    private void testRunInternal() throws Exception {
        configureBackups();
        configureRecordStore();

        // HDPutAllOperation
        HDPutAllOperation operation = runAndGetHDPutAllOperation();
        assertBackupConfiguration(operation);

        verifyRecordStoreAfterOperation();
        verifyNearCacheInvalidatorAfterOperation();

        // there is a single forced eviction for the thrown NativeOOME
        if (throwNativeOOME) {
            verify(hdEvictor).forceEvict(eq(recordStore));
        }
        verifyZeroInteractions(hdEvictor);

        // HDPutAllBackupOperation
        if (syncBackupCount > 0) {
            runHDPutAllBackupOperation(operation);

            verifyRecordStoreAfterBackupOperation();
            verifyZeroInteractions(hdEvictor);
        }
    }

    /**
     * Configures the number of sync backups.
     */
    private void configureBackups() {
        when(mapContainer.getBackupCount()).thenReturn(syncBackupCount);
        when(mapContainer.getTotalBackupCount()).thenReturn(syncBackupCount);
    }

    /**
     * Configures the {@link RecordStore} mock.
     */
    private void configureRecordStore() {
        OngoingStubbing<Boolean> stub = when(recordStore.set(any(Data.class), any(), anyLong()));
        if (throwNativeOOME) {
            stub.thenReturn(true, true, true).thenThrow(new NativeOutOfMemoryError()).thenReturn(true);
        } else {
            stub.thenReturn(true);
        }
    }

    /**
     * Creates a {@link HDPutAllOperation} with the necessary mocks and settings.
     *
     * @return the {@link HDPutAllOperation}
     * @throws Exception if {@link HDMapOperation#beforeRun()} throws an exception
     */
    private HDPutAllOperation runAndGetHDPutAllOperation() throws Exception {
        HDPutAllOperation operation = new HDPutAllOperation(MAP_NAME, mapEntries);
        operation.setMapService(mapService);
        operation.setService(mapService);
        operation.setPartitionId(PARTITION_ID);

        operation.beforeRun();
        operation.run();
        operation.afterRun();

        return operation;
    }

    /**
     * Creates a {@link HDPutAllBackupOperation} with the necessary mocks and settings.
     *
     * @throws Exception if {@link HDMapOperation#beforeRun()} throws an exception
     */
    private void runHDPutAllBackupOperation(HDPutAllOperation operation) throws Exception {
        HDPutAllBackupOperation backupOperation = (HDPutAllBackupOperation) operation.getBackupOperation();
        backupOperation.setMapService(mapService);
        backupOperation.setService(mapService);
        backupOperation.setPartitionId(PARTITION_ID);

        backupOperation.beforeRun();
        backupOperation.run();
        backupOperation.afterRun();
    }

    /**
     * Asserts the backup configuration of the {@link HDPutAllOperation}.
     *
     * @param operation the {@link HDPutAllOperation} to check
     */
    private void assertBackupConfiguration(HDPutAllOperation operation) {
        if (syncBackupCount == 0) {
            assertFalse(operation.shouldBackup());
            assertEquals(0, operation.getSyncBackupCount());
            assertEquals(0, operation.getAsyncBackupCount());
        } else {
            assertTrue(operation.shouldBackup());
            assertEquals(1, operation.getSyncBackupCount());
            assertEquals(0, operation.getAsyncBackupCount());
        }
    }

    /**
     * Verifies the {@link RecordStore} mock after a {@link HDPutAllOperation#run()} call.
     */
    private void verifyRecordStoreAfterOperation() {
        int numberOfExceptions = (throwNativeOOME ? 1 : 0);

        // RecordStore.getMapContainer() is called again for each NativeOOME during the forced eviction
        verify(recordStore, times(1 + numberOfExceptions)).getMapContainer();
        // RecordStore.set() is called again for each entry which threw a NativeOOME
        verify(recordStore, times(ITEM_COUNT + numberOfExceptions)).set(any(Data.class), any(), anyLong());

        verify(recordStore, times(ITEM_COUNT * syncBackupCount)).getRecord(any(Data.class));
        verify(recordStore, times(ITEM_COUNT)).evictEntries();
        verify(recordStore, times(ITEM_COUNT)).getMapDataStore();

        verifyNoMoreInteractions(recordStore);
    }

    /**
     * Verifies the {@link RecordStore} mock after a {@link HDPutAllBackupOperation#run()}.
     */
    private void verifyRecordStoreAfterBackupOperation() {
        int numberOfExceptions = (throwNativeOOME ? 1 : 0);

        // RecordStore.getMapContainer() is called again for each NativeOOME during the forced eviction
        verify(recordStore, times(2 + numberOfExceptions)).getMapContainer();

        verify(recordStore, times(ITEM_COUNT)).putBackup(any(Data.class), any());
        verify(recordStore, times(ITEM_COUNT * 2)).evictEntries();
        verifyNoMoreInteractions(recordStore);
    }

    /**
     * Verifies the {@link NearCacheInvalidator} mock after a {@link HDPutAllOperation#afterRun()} call.
     */
    @SuppressWarnings("unchecked")
    private void verifyNearCacheInvalidatorAfterOperation() {
        ArgumentCaptor<List<Data>> keysCaptor = ArgumentCaptor.forClass((Class) List.class);
        verify(nearCacheInvalidator).invalidate(eq(MAP_NAME), keysCaptor.capture(), anyString());
        verifyNoMoreInteractions(nearCacheInvalidator);

        List<Data> keys = keysCaptor.getAllValues().get(0);
        assertEquals("NearCacheInvalidator should have received all keys", ITEM_COUNT, keys.size());
    }
}
