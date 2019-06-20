package com.hazelcast.test.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.partition.TestPartitionUtils;
import com.hazelcast.internal.partition.impl.PartitionServiceState;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;

import static com.hazelcast.test.HazelcastTestSupport.assertEqualsStringFormat;
import static com.hazelcast.test.HazelcastTestSupport.ignore;
import static java.lang.String.format;
import static java.lang.reflect.Proxy.isProxyClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CompatibilityTestHazelcastInstanceFactoryTest {

    /**
     * The default cluster size is the number of released versions plus the
     * current version, but this can be overridden via a system property.
     */
    private static final int CLUSTER_SIZE = 3;

    /**
     * The expected index of the current version in the array of created
     * Hazelcast instances.
     */
    private static final int EXPECTED_INDEX_OF_CURRENT_VERSION = CLUSTER_SIZE - 1;

    private TestHazelcastInstanceFactory factory;

    @After
    public void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    @Test
    public void testHazelcastFactoryCompatibility_withCompatibilityTestHazelcastInstanceFactory_withNewInstances() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
        assertEqualsStringFormat("Expected %s Hazelcast versions in the factory, but found %s", CLUSTER_SIZE, factory.getCount());
        testInstanceFactory(true, true);
    }

    @Test
    public void testHazelcastFactoryCompatibility_withCompatibilityTestHazelcastInstanceFactory_withNewHazelcastInstance() {
        factory = new CompatibilityTestHazelcastInstanceFactory();
        assertEqualsStringFormat("Expected %s Hazelcast versions in the factory, but found %s", CLUSTER_SIZE, factory.getCount());
        testInstanceFactory(true, false);
    }

    @Test
    public void testHazelcastFactoryCompatibility_withTestHazelcastInstanceFactory_withNewInstances() {
        factory = new TestHazelcastInstanceFactory(CLUSTER_SIZE);
        testInstanceFactory(false, true);
    }

    @Test
    public void testHazelcastFactoryCompatibility_withTestHazelcastInstanceFactory_withNewHazelcastInstance() {
        factory = new TestHazelcastInstanceFactory(CLUSTER_SIZE);
        testInstanceFactory(false, false);
    }

    private void testInstanceFactory(boolean hasProxyClasses, boolean useNewInstances) {
        HazelcastInstance[] hazelcastInstances = getHazelcastInstances(useNewInstances);
        assertEqualsStringFormat("Expected %s Hazelcast instances, but found %s", CLUSTER_SIZE, hazelcastInstances.length);
        assertEqualsStringFormat("Expected %s active Hazelcast instances in the factory, but found %s",
                CLUSTER_SIZE, factory.getAllHazelcastInstances().size());

        // test if the expected Hazelcast instances are a proxy class
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Class<? extends HazelcastInstance> clazz = hazelcastInstances[i].getClass();
            if (hasProxyClasses && i != EXPECTED_INDEX_OF_CURRENT_VERSION) {
                assertTrue(format("Hazelcast instance at index %d should be a proxied class, but was %s (classloader: %s)",
                        i, clazz, clazz.getClassLoader()), isProxyClass(clazz));
            } else {
                assertFalse(format("Hazelcast instance at index %d shouldn't be a proxied class, but was %s (classloader: %s)",
                        i, clazz, clazz.getClassLoader()), isProxyClass(clazz));
            }
        }

        // shutdown one instance after another to test factory.getAllHazelcastInstances(),
        // which should only return active Hazelcast instances
        Iterator<HazelcastInstance> iterator = factory.getAllHazelcastInstances().iterator();
        while (iterator.hasNext()) {
            HazelcastInstance hz = iterator.next();

            Node proxyNode = TestUtil.getNode(hz);
            HazelcastInstanceImpl proxyInstanceImpl = TestUtil.getHazelcastInstanceImpl(hz);

            assertNode(proxyNode, true);
            assertHazelcastInstanceImpl(proxyInstanceImpl);
            assertSafePartitionServiceState(hz);

            hz.shutdown();
            assertNode(proxyNode, false);

            assertGetNodeFromShutdownInstance(hz);
            assertGetHazelcastInstanceImplFromShutdownInstance(hz);
            assertSafePartitionServiceState(hz);

            iterator = factory.getAllHazelcastInstances().iterator();
        }

        assertEqualsStringFormat("Expected %s active Hazelcast instances in the factory, but found %s",
                0, factory.getAllHazelcastInstances().size());
    }

    private HazelcastInstance[] getHazelcastInstances(boolean useNewInstances) {
        if (useNewInstances) {
            return factory.newInstances();
        }
        HazelcastInstance[] instances = new HazelcastInstance[CLUSTER_SIZE];
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance();
        }
        return instances;
    }

    private static void assertNode(Node node, boolean isRunning) {
        assertNotNull(node);
        assertEquals(isRunning, node.isRunning());
    }

    private static void assertGetNodeFromShutdownInstance(HazelcastInstance hz) {
        try {
            TestUtil.getNode(hz);
            fail("Expected IllegalArgumentException from TestUtil.getNode()");
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
    }

    private static void assertHazelcastInstanceImpl(HazelcastInstanceImpl hazelcastInstanceImpl) {
        assertNotNull(hazelcastInstanceImpl);
    }

    private static void assertGetHazelcastInstanceImplFromShutdownInstance(HazelcastInstance hz) {
        try {
            TestUtil.getHazelcastInstanceImpl(hz);
            fail("Expected IllegalArgumentException from TestUtil.getHazelcastInstanceImpl()");
        } catch (IllegalArgumentException expected) {
            ignore(expected);
        }
    }

    private static void assertSafePartitionServiceState(HazelcastInstance realInstance) {
        PartitionServiceState partitionServiceState = TestPartitionUtils.getPartitionServiceState(realInstance);
        assertEquals(PartitionServiceState.SAFE, partitionServiceState);
    }
}
