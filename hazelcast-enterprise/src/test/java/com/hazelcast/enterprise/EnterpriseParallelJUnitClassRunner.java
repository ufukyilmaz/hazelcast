package com.hazelcast.enterprise;

import com.hazelcast.internal.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;

/**
 * @mdogan 7/20/12
 */
public class EnterpriseParallelJUnitClassRunner extends HazelcastParallelClassRunner {

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

    public EnterpriseParallelJUnitClassRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    public EnterpriseParallelJUnitClassRunner(Class<?> clazz, Object[] parameters, String name) throws InitializationError {
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
