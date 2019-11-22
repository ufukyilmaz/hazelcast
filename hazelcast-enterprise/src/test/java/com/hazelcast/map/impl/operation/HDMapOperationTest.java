package com.hazelcast.map.impl.operation;

import com.hazelcast.HDTestSupport;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionException;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapOperationTest extends HazelcastTestSupport {

    @Test(expected = UnsupportedOperationException.class)
    public void testGetThreadId() {
        run(MAP_NAMES.GET_THREAD_ID);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetThreadId() {
        run(MAP_NAMES.SET_THREAD_ID);
    }

    @Test
    public void testLogError_withNormalException() {
        run(MAP_NAMES.LOG_NORMAL_EXCEPTION);
    }

    @Test
    public void testLogError_withNativeOutOfMemoryError() {
        run(MAP_NAMES.LOG_NATIVE_OOME);
    }

    private void run(MAP_NAMES mapNames) {
        String mapName = mapNames.name();

        MapConfig mainMapConfig = new MapConfig(mapName);
        mainMapConfig.setInMemoryFormat(InMemoryFormat.NATIVE)
                .getEvictionConfig().setEvictionPolicy(EvictionPolicy.LFU);

        Config hdConfig = HDTestSupport.getHDConfig();
        hdConfig.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        hdConfig.addMapConfig(mainMapConfig);

        HazelcastInstance node = createHazelcastInstance(hdConfig);

        // run operation
        try {
            TestHDOperation operation = new TestHDOperation(mapName);
            getOperationService(node)
                    .createInvocationBuilder(MapService.SERVICE_NAME, operation, 0)
                    .invoke()
                    .join();
        } catch (CompletionException e) {
            throw ExceptionUtil.rethrow(e.getCause());
        }
    }

    private enum MAP_NAMES {
        GET_THREAD_ID,
        SET_THREAD_ID,
        LOG_NORMAL_EXCEPTION,
        LOG_NATIVE_OOME
    }

    private static class TestHDOperation extends MapOperation {

        TestHDOperation() {
        }

        TestHDOperation(String name) {
            super(name);
        }

        @Override
        protected void runInternal() {
            switch (MAP_NAMES.valueOf(name)) {
                case GET_THREAD_ID:
                    getThreadId();
                    break;
                case SET_THREAD_ID:
                    setThreadId(Thread.currentThread().getId());
                    break;
                case LOG_NORMAL_EXCEPTION:
                    logError(new RuntimeException("expected exception"));
                    break;
                case LOG_NATIVE_OOME:
                    logError(new NativeOutOfMemoryError("expected exception"));
                    break;
            }
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }
}
