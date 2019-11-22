package com.hazelcast.map.impl.operation;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDPutOperationsTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 6;
    private static final String MAP_NAME = "HDPutAllOperationTest";

    private final AtomicInteger addedEntryCount = new AtomicInteger();
    private final AtomicInteger updatedEntryCount = new AtomicInteger();

    @Test
    public void test_put_run_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice() {
        Operation operation = new HDPutOperation(MAP_NAME, new HeapData(), new HeapData());
        testRunInternal(operation);

        assertEqualsEventually(1, addedEntryCount);
        assertTrueAllTheTime(() -> assertEquals(0, updatedEntryCount.get()), 3);
    }

    @Test
    public void test_putAll_run_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice() {
        Operation putAllOperation = createPutAllOperation();
        testRunInternal(putAllOperation);

        assertEqualsEventually(ENTRY_COUNT, addedEntryCount);
        assertTrueAllTheTime(() -> assertEquals(0, updatedEntryCount.get()), 3);
    }

    private void testRunInternal(Operation op) {
        // main map's config
        MapConfig mainMapConfig = new MapConfig(MAP_NAME);
        mainMapConfig.setInMemoryFormat(InMemoryFormat.NATIVE)
                .getEvictionConfig().setEvictionPolicy(EvictionPolicy.LFU);

        Config hdConfig = HDTestSupport.getHDConfig();
        hdConfig.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        hdConfig.addMapConfig(mainMapConfig);

        HazelcastInstance node = createHazelcastInstance(hdConfig);
        IMap map = node.getMap(MAP_NAME);
        map.addEntryListener((EntryAddedListener) event -> {
            addedEntryCount.incrementAndGet();
        }, false);
        map.addEntryListener((EntryUpdatedListener) event -> {
            updatedEntryCount.incrementAndGet();
        }, false);

        // run operation
        try {
            getOperationService(node)
                    .createInvocationBuilder(MapService.SERVICE_NAME, op, 0)
                    .invoke()
                    .join();
        } catch (CompletionException e) {
            throw ExceptionUtil.rethrow(e.getCause());
        }
    }

    private Operation createPutAllOperation() {
        MapEntries mapEntries = createMapEntries(ENTRY_COUNT);
        return new HDPutAllOperation(MAP_NAME, mapEntries,
                ENTRY_COUNT / 2);
    }

    /**
     * Creates a {@link MapEntries} instances with mocked {@link Data} entries.
     *
     * @param itemCount number of entries
     * @return a {@link MapEntries} instance
     */
    private static MapEntries createMapEntries(int itemCount) {
        Random random = new Random();
        MapEntries mapEntries = new MapEntries(itemCount);
        for (int i = 0; i < itemCount; i++) {
            byte[] bytes = new byte[8];
            random.nextBytes(bytes);

            Data key = new HeapData(bytes);
            Data value = new HeapData(bytes);

            mapEntries.add(key, value);
        }
        return mapEntries;
    }

    private static class HDPutAllOperation extends PutAllOperation {

        private int putNumberToThrowException;
        private int putCountSoFar;

        HDPutAllOperation(String name, MapEntries mapEntries, int putNumberToThrowException) {
            super(name, mapEntries);
            this.putNumberToThrowException = putNumberToThrowException;
        }

        @Override
        protected void put(Data dataKey, Data dataValue) {
            if (++putCountSoFar == putNumberToThrowException) {
                throw new NativeOutOfMemoryError("Expected NativeOutOfMemoryError");
            }
            super.put(dataKey, dataValue);
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    private static class HDPutOperation extends PutOperation {

        private int maxNoomeCount;

        HDPutOperation(String name, Data dataKey, Data value) {
            super(name, dataKey, value);
        }

        @Override
        protected void runInternal() {
            super.runInternal();

            if (maxNoomeCount++ < 1) {
                throw new NativeOutOfMemoryError("Expected NativeOutOfMemoryError");
            }
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }
}
