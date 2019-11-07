package com.hazelcast.client.cache.eviction;

import com.hazelcast.cache.eviction.HiDensityCacheEvictionPolicyComparatorTest;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cache.CacheTestSupport.createClientCachingProvider;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHiDensityCacheEvictionPolicyComparatorTest extends HiDensityCacheEvictionPolicyComparatorTest {

    private final TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
    private HazelcastInstance instance;

    @Override
    protected CachingProvider createCachingProvider(HazelcastInstance instance) {
        return createClientCachingProvider(instance);
    }

    @Override
    protected HazelcastInstance createInstance(Config config) {
        instance = instanceFactory.newHazelcastInstance(config);
        return instanceFactory.newHazelcastClient();
    }

    @Override
    protected ConcurrentMap getUserContext(HazelcastInstance hazelcastInstance) {
        return instance.getUserContext();
    }

    @After
    public void tearDown() {
        instanceFactory.shutdownAll();
    }
}
