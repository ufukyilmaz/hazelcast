package com.hazelcast.wan.cache;

import com.hazelcast.cache.CacheMergePolicy;
import com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.cache.impl.merge.policy.CacheMergePolicyProvider}.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheMergePolicyProviderTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TestHazelcastInstanceFactory factory;
    private CacheMergePolicyProvider cacheMergePolicyProvider;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(1);

        HazelcastInstance instance = factory.newHazelcastInstance();
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        cacheMergePolicyProvider = new CacheMergePolicyProvider(nodeEngineImpl);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = InvalidConfigurationException.class)
    public void nullMergePolicyCheck() {
        cacheMergePolicyProvider.getMergePolicy(null);
    }

    @Test
    public void nonExistingMergePolicyCheck() {
        expectedException.expect(new RootCauseMatcher(ClassNotFoundException.class, "test"));
        cacheMergePolicyProvider.getMergePolicy("test");
    }

    @Test
    public void outOfTheBoxMergePolicyCheck() {
        Object mergePolicy = cacheMergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        assertTrue(mergePolicy instanceof SplitBrainMergePolicy);
        assertTrue(mergePolicy instanceof PutIfAbsentMergePolicy);
    }

    @Test
    public void customCacheMergePolicyCheck() {
        Object mergePolicy = cacheMergePolicyProvider.getMergePolicy(CustomCacheMergePolicy.class.getName());
        assertTrue(mergePolicy instanceof CacheMergePolicy);
        assertTrue(mergePolicy instanceof CustomCacheMergePolicy);
    }
}
