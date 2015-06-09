package com.hazelcast.wan.cache;

import com.hazelcast.cache.merge.CacheMergePolicy;
import com.hazelcast.cache.merge.CacheMergePolicyProvider;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link com.hazelcast.cache.merge.CacheMergePolicyProvider}
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CacheMergePolicyProviderTest extends HazelcastTestSupport{

    HazelcastInstance instance;
    TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(1);
        instance = factory.newHazelcastInstance();
    }

    @Test(expected = NullPointerException.class)
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
