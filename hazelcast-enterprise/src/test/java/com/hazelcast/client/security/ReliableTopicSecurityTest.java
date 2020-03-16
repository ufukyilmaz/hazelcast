package com.hazelcast.client.security;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.randomString;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReliableTopicSecurityTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final TestHazelcastFactory factory = new TestHazelcastFactory();
    private final String testObjectName = randomString();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testTopicPublishPermissionPass() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_PUBLISH).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getReliableTopic(testObjectName).publish("value");
    }

    @Test
    public void testTopicListenerPermissionPass() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_LISTEN).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        client.getReliableTopic(testObjectName).addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {

            }
        });
    }

    @Test
    public void testTopicPublishPermissionFail() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_LISTEN).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getReliableTopic(testObjectName).publish("value");
    }

    @Test
    public void testTopicListenerPermissionFail() {
        final Config config = createConfig();
        addPermission(config, testObjectName)
                .addAction(ActionConstants.ACTION_ADD).addAction(ActionConstants.ACTION_CREATE);
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        expectedException.expect(RuntimeException.class);
        client.getReliableTopic(testObjectName).addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {

            }
        });
    }

    private Config createConfig() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        return config;
    }

    private PermissionConfig addPermission(Config config, String name) {
        PermissionConfig perm = new PermissionConfig(PermissionConfig.PermissionType.RELIABLE_TOPIC, name, "dev");
        config.getSecurityConfig().addClientPermissionConfig(perm);
        return perm;
    }

}
