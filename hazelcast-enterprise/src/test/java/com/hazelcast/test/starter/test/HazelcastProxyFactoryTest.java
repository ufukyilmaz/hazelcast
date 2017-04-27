package com.hazelcast.test.starter.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.starter.HazelcastAPIDelegatingClassloader;
import com.hazelcast.test.starter.HazelcastProxyFactory;
import com.hazelcast.test.starter.HazelcastStarter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HazelcastProxyFactoryTest {

    @Test
    public void testRetunedProxyImplements_sameInterfaceByNameOnTargetClassLoader()
            throws Exception {
        ProxiedInterface delegate = new ProxiedInterface() {
            @Override
            public void get() {
            }
        };

        // HazelcastAPIDelegatingClassloader will reload the bytes of ProxiedInterface as a new class
        // as happens with every com.hazelcast class that contains "test"
        HazelcastAPIDelegatingClassloader targetClassLoader =
                new HazelcastAPIDelegatingClassloader(new URL[] {}, HazelcastProxyFactoryTest.class.getClassLoader());

        Object proxy = HazelcastProxyFactory.proxyObjectForStarter(targetClassLoader, delegate);

        assertNotNull(proxy);
        Class<?>[] ifaces = proxy.getClass().getInterfaces();
        assertEquals(1, ifaces.length);

        Class<?> proxyInterface = ifaces[0];
        // it is not the same class but has the same name on a different classloader
        assertNotEquals(ProxiedInterface.class, proxyInterface);
        assertEquals(ProxiedInterface.class.getName(), proxyInterface.getName());
        assertEquals(targetClassLoader, proxyInterface.getClassLoader());
    }

    @Test
    public void testProxyHazelcastInstanceClasses_ofSameVersion_areSame() {
        HazelcastInstance hz1 = HazelcastStarter.newHazelcastInstance("3.8");
        HazelcastInstance hz2 = HazelcastStarter.newHazelcastInstance("3.8");
        assertEquals(hz1.getClass(), hz2.getClass());
    }

    public interface ProxiedInterface {
        void get();
    }
}
