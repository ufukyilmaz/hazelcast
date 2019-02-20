package com.hazelcast.enterprise;

import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * Test runner which sets the enterprise key license to an unlimited key license and runs the test in series.
 */
public class EnterpriseSerialJUnitClassRunner extends HazelcastSerialClassRunner {

    static {
        /**
         * {@link com.hazelcast.memory.PoolingMemoryManager}
         * (actually {@link com.hazelcast.memory.GlobalPoolingMemoryManager)
         * uses a bitmap for detecting none-page addresses with less complexity.
         * By default it is {@link com.hazelcast.memory.GlobalPoolingMemoryManager#PAGE_LOOKUP_SIZE),
         * but we should reduce its memory foot-print in the tests
         * because some tests uses very small native memory and the test itself relies that size.
         * Otherwise, we might be faced with native OOME.
         * Also, indexing as least significant 16-bits is enough for our test environment.
         */
        System.setProperty("hazelcast.memory.pageLookupLength", String.valueOf(1 << 16));
    }

    public EnterpriseSerialJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    public EnterpriseSerialJUnitClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
        super(clazz, parameters, name);
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        super.runChild(method, notifier);
    }

    @Override
    public void run(RunNotifier notifier) {
        GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty(UNLIMITED_LICENSE);
        super.run(notifier);
    }
}