package com.hazelcast.internal.serialization.impl;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;

import static com.hazelcast.enterprise.SampleLicense.ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART;
import static org.junit.Assert.assertEquals;


@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRollingUpgradeCompatibilityTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private HazelcastInstance instance;

    @Before
    public void init() {
        Config config = new Config();
        config.setProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName(), ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART);
        instance = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(1, instance);
    }

    @After
    public void stop() {
        instance.shutdown();
    }

    private SerializationService getClientSerializationService() {
        ClientConfig clientConfig = new ClientConfig();
        HazelcastClientProxy client = (HazelcastClientProxy) factory.newHazelcastClient(clientConfig);
        return client.getSerializationService();
    }

    @Test
    public void checkIfProperSerializerUsed_withoutRollingUpgrades() {
        AbstractSerializationService ss = (AbstractSerializationService) getClientSerializationService();

        SerializerAdapter serializerAdapter = get("dataSerializerAdapter", ss);

        assertEquals(DataSerializableSerializer.class, serializerAdapter.getImpl().getClass());
    }

    private static <T> T get(String fieldName, Object object) {
        Class myClass = object.getClass();
        Field myField;
        try {
            myField = getField(myClass, fieldName);
            myField.setAccessible(true);
            return (T) myField.get(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Field getField(Class clazz, String fieldName)
            throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw e;
            } else {
                return getField(superClass, fieldName);
            }
        }
    }

}


