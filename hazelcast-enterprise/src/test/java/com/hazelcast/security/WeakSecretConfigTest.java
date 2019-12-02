package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.security.impl.WeakSecretError;
import com.hazelcast.security.impl.WeakSecretsConfigChecker;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WeakSecretConfigTest {

    @Test
    public void testEmptyConfig() {
        Config config = new Config();

        WeakSecretsConfigChecker checker = new WeakSecretsConfigChecker(config);
        Map<String, EnumSet<WeakSecretError>> weaknesses = checker.evaluate();
        assertTrue(weaknesses.isEmpty());
    }

    @Test
    public void testSSLConfig() {
        Config config = new Config();

        Properties properties = new Properties();
        properties.put("keyStore", "keyStore");
        properties.put("keyStorePassword", "bob");
        properties.put("trustStorePassword", "abby");
        properties.put("protocol", "TLS");

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperties(properties).setEnabled(true);

        config.getNetworkConfig().setSSLConfig(sslConfig);

        WeakSecretsConfigChecker checker = new WeakSecretsConfigChecker(config);
        Map<String, EnumSet<WeakSecretError>> weaknesses = checker.evaluate();
        assertFalse(weaknesses.isEmpty());
        assertTrue(weaknesses.containsKey("SSLConfig property[keyStorePassword]"));
        assertTrue(weaknesses.containsKey("SSLConfig property[trustStorePassword]"));
    }

    @Test
    public void testSSLConfig_whenDisabled() {
        Config config = new Config();

        Properties properties = new Properties();
        properties.put("keyStore", "keyStore");
        properties.put("keyStorePassword", "bob");
        properties.put("trustStorePassword", "abby");
        properties.put("protocol", "TLS");

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperties(properties).setEnabled(false);

        config.getNetworkConfig().setSSLConfig(sslConfig);

        WeakSecretsConfigChecker checker = new WeakSecretsConfigChecker(config);
        Map<String, EnumSet<WeakSecretError>> weaknesses = checker.evaluate();
        assertTrue(weaknesses.isEmpty());
    }

    @Test
    public void testSSLConfig_whenAdvancedNetwork() {
        Config config = new Config();

        Properties properties = new Properties();
        properties.put("keyStore", "keyStore");
        properties.put("keyStorePassword", "bob");
        properties.put("trustStorePassword", "abby");
        properties.put("protocol", "TLS");

        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setProperties(properties).setEnabled(true);

        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER).setSSLConfig(sslConfig);

        WeakSecretsConfigChecker checker = new WeakSecretsConfigChecker(config);
        Map<String, EnumSet<WeakSecretError>> weaknesses = checker.evaluate();
        assertFalse(weaknesses.isEmpty());
        assertTrue(weaknesses.containsKey("SSLConfig property[keyStorePassword]"));
        assertTrue(weaknesses.containsKey("SSLConfig property[trustStorePassword]"));
    }

}
