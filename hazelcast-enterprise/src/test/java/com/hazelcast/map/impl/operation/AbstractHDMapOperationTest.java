package com.hazelcast.map.impl.operation;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.eviction.HDEvictorImpl;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.nearcache.EnterpriseMapNearCacheManager;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.wan.impl.CallerProvenance;
import org.junit.Before;
import org.mockito.stubbing.OngoingStubbing;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.TRUE_FILTER;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.operation.AbstractHDMapOperationTest.OperationType.PUT;
import static com.hazelcast.map.impl.operation.AbstractHDMapOperationTest.OperationType.PUT_ALL;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractHDMapOperationTest {

    static final int ENTRY_COUNT = 5;
    static final int PARTITION_COUNT = 3;
    static final int PARTITION_ID = 23;

    enum OperationType {
        PUT_ALL,
        PUT
    }

    int syncBackupCount;
    boolean throwNativeOOME;

    HDEvictorImpl evictor;
    RecordStore recordStore;
    ConcurrentHashMap<String, RecordStore> partitionMaps;
    MapService mapService;
    NodeEngine nodeEngine;

    private int numberOfNativeOOME;


    private MapContainer mapContainer;
    private TestInvalidator nearCacheInvalidator;

    @Before
    public void setUp() {
        IPartitionService partitionService = mock(IPartitionService.class);
        when(partitionService.getPartitionCount()).thenReturn(PARTITION_COUNT);

        OperationService operationService = mock(OperationService.class);
        when(operationService.getPartitionThreadCount()).thenReturn(PARTITION_COUNT);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(Logger.getLogger(getClass()));
        when(nodeEngine.getPartitionService()).thenReturn(partitionService);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getProperties()).thenReturn(new HazelcastProperties(new Properties()));

        MapConfig mapConfig = new MapConfig(getMapName())
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionPolicy(EvictionPolicy.LRU);

        evictor = mock(HDEvictorImpl.class);

        mapContainer = mock(MapContainer.class);
        when(mapContainer.getWanReplicationPublisher()).thenReturn(null);
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        when(mapContainer.getEvictor()).thenReturn(evictor);
        when(mapContainer.hasInvalidationListener()).thenReturn(true);

        MapDataStore mapDataStore = mock(MapDataStore.class);

        Record record = mock(Record.class);

        recordStore = mock(RecordStore.class);
        when(recordStore.getMapDataStore()).thenReturn(mapDataStore);
        when(recordStore.getMapContainer()).thenReturn(mapContainer);
        when(recordStore.getRecord(any(Data.class))).thenReturn(record);
        when(recordStore.putBackup(any(Data.class), any(), any(CallerProvenance.class))).thenReturn(record);
        when(recordStore.putBackup(any(Data.class), any(Data.class), anyLong(), anyLong(), anyBoolean(), any(CallerProvenance.class))).thenReturn(record);

        partitionMaps = new ConcurrentHashMap<String, RecordStore>();

        PartitionContainer partitionContainer = mock(PartitionContainer.class);
        when(partitionContainer.getRecordStore(eq(getMapName()))).thenReturn(recordStore);
        when(partitionContainer.getMaps()).thenReturn(partitionMaps);

        MapEventPublisher mapEventPublisher = mock(MapEventPublisher.class);
        when(mapEventPublisher.hasEventListener(eq(getMapName()))).thenReturn(false);

        EnterpriseMapNearCacheManager nearCacheManager = mock(EnterpriseMapNearCacheManager.class, RETURNS_DEEP_STUBS);

        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        when(mapServiceContext.getPartitionContainer(geq(1))).thenReturn(partitionContainer);
        when(mapServiceContext.getMapEventPublisher()).thenReturn(mapEventPublisher);
        when(mapServiceContext.getMapNearCacheManager()).thenReturn(nearCacheManager);
        when(mapServiceContext.getMapContainer(eq(getMapName()))).thenReturn(mapContainer);
        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);

        nearCacheInvalidator = new TestInvalidator(mapServiceContext);
        when(nearCacheManager.getInvalidator()).thenReturn(nearCacheInvalidator);

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
            Data key = new HeapData(new byte[]{});
            Data value = new HeapData(new byte[]{});
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
                OngoingStubbing<Boolean> setStub = when(recordStore.set(any(Data.class), any(), anyLong(), anyLong()) != null);
                if (throwNativeOOME) {
                    setStub.thenReturn(true, true, true).thenThrow(exception).thenReturn(true);
                    numberOfNativeOOME = 1;
                } else {
                    setStub.thenReturn(true);
                    numberOfNativeOOME = 0;
                }
                return;

            case PUT:
                OngoingStubbing<Object> putStub = when(recordStore.put(any(Data.class), any(Data.class), anyLong(), anyLong()));
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
     * @throws Exception if {@link Operation#beforeRun()} or {@link Operation#afterRun()} throws an exception
     */
    void executeOperation(Operation operation, int partitionId) throws Exception {
        prepareOperation(operation, partitionId);

        operation.beforeRun();
        operation.run();
        operation.afterRun();
    }

    void prepareOperation(Operation operation, int partitionId) {
        if (operation instanceof MapOperation) {
            MapOperation mapOperation = (MapOperation) operation;
            mapOperation.setMapService(mapService);
        }
        operation.setService(mapService);
        operation.setNodeEngine(nodeEngine);
        operation.setCallerUuid(newUnsecureUuidString());
        operation.setPartitionId(partitionId);
    }

    /**
     * Verifies the {@link RecordStore} mock after a call of {@link PutAllOperation#run()} or
     * {@link PutAllBackupOperation#run()}.
     *
     * @param isBackupDone {@code true} if backup operation has been called, {@code false} otherwise
     */
    void verifyRecordStoreAfterRun(OperationType operationType, boolean isBackupDone) {
        boolean verifyBackups = syncBackupCount > 0 && isBackupDone;
        int backupFactor = (verifyBackups ? 2 : 1);

        if (operationType == PUT_ALL) {
            // RecordStore.set() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).set(any(Data.class), any(), anyLong(), anyLong());
        } else if (operationType == PUT) {
            // RecordStore.put() is called again for each entry which threw a NativeOOME
            verify(recordStore, times(ENTRY_COUNT + numberOfNativeOOME)).put(any(Data.class), any(), anyLong(), anyLong());
        }

        verify(recordStore, times(ENTRY_COUNT * syncBackupCount)).getRecord(any(Data.class));
        verify(recordStore, times(ENTRY_COUNT * backupFactor)).evictEntries(any(Data.class));
        verify(recordStore, atLeast(ENTRY_COUNT)).getMapDataStore();
        verify(recordStore, atLeastOnce()).getMapContainer();

        if (verifyBackups) {
            if (operationType == PUT_ALL) {
                verify(recordStore, times(ENTRY_COUNT)).putBackup(any(Data.class), any(), any(CallerProvenance.class));
            } else {
                verify(recordStore, times(ENTRY_COUNT)).putBackup(any(Data.class), any(Data.class), anyLong(), anyLong(), anyBoolean(), any(CallerProvenance.class));
            }
        }

        verifyNoMoreInteractions(recordStore);
    }

    /**
     * Verifies the {@link Invalidator} mock after a {@link HDPutAllOperation#afterRun()} call.
     */
    void verifyNearCacheInvalidatorAfterRun() {
        assertEquals("NearCacheInvalidator should have received all keys", nearCacheInvalidator.getCount(), ENTRY_COUNT);
    }

    /**
     * Verifies that there is a single forced eviction if a NativeOOME has been thrown
     */
    void verifyHDEvictor(OperationType operationType) {
        if (throwNativeOOME) {
            int expectedCount = (operationType == PUT) ? ENTRY_COUNT : 1;
            verify(evictor, times(expectedCount)).forceEvict(eq(recordStore));
            verifyNoMoreInteractions(evictor);
        } else {
            verifyZeroInteractions(evictor);
        }
    }

    abstract String getMapName();

    private static class TestInvalidator extends Invalidator {

        private final AtomicInteger count = new AtomicInteger();

        TestInvalidator(MapServiceContext mapServiceContext) {
            super(SERVICE_NAME, TRUE_FILTER, mapServiceContext.getNodeEngine());
        }

        @Override
        protected void invalidateInternal(Invalidation invalidation, int i) {
            count.incrementAndGet();
        }

        public int getCount() {
            return count.get();
        }
    }
}
