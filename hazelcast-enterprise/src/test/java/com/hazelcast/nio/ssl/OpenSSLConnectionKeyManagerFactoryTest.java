package com.hazelcast.nio.ssl;

import static com.hazelcast.TestEnvironmentUtil.isIbmJvm;
import static org.junit.Assume.assumeFalse;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class })
public class OpenSSLConnectionKeyManagerFactoryTest extends OpenSSLConnectionTest {

    @BeforeClass
    public static void beforeClass() {
        assumeFalse(isIbmJvm());
    }

    protected Config newConfig() {
        Properties sslProperties = TestKeyStoreUtil.createSslProperties();

        SSLConfig sslConfig = new SSLConfig()
                .setEnabled(true)
                .setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperties(sslProperties);

        Config config = new Config().setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
