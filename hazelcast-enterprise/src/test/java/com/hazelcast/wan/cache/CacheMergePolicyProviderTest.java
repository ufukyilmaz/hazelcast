package com.hazelcast.wan.cache;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider}
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CacheMergePolicyProviderTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(1);
        instance = factory.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void nullMergePolicyCheck() {
        CacheMergePolicyProvider cacheMergePolicyProvider = new CacheMergePolicyProvider(getNodeEngineImpl(instance));
        cacheMergePolicyProvider.getMergePolicy(null);
    }

    @Test(expected = ClassNotFoundException.class)
    public void nonExistingMergePolicyCheck() throws Throwable {
        CacheMergePolicyProvider cacheMergePolicyProvider = new CacheMergePolicyProvider(getNodeEngineImpl(instance));
        try {
            cacheMergePolicyProvider.getMergePolicy("test");
        } catch (Throwable ex) {
            throw ex.getCause();
        }
    }

    @Test
    public void customMergePolicyCheck() {
        CacheMergePolicyProvider cacheMergePolicyProvider = new CacheMergePolicyProvider(getNodeEngineImpl(instance));
        CacheMergePolicy mergePolicy = cacheMergePolicyProvider.getMergePolicy(CustomCacheMergePolicy.class.getName());
        assertTrue(mergePolicy instanceof CustomCacheMergePolicy);
    }
}
