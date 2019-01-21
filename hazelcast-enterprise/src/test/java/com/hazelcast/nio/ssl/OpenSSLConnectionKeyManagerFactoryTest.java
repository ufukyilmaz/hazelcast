package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.TestEnvironmentUtil.isIbmJvm;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assume.assumeFalse;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class OpenSSLConnectionKeyManagerFactoryTest extends OpenSSLConnectionTest {

    @BeforeClass
    public static void beforeClass() {
        assumeFalse(isIbmJvm());
    }

    protected Config newConfig() {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true);
        File letsEncryptKeystore = copyTestResource(OpenSSLConnectionKeyManagerFactoryTest.class, tempFolder.getRoot(), "server.jks");
        sslConfig.setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperty("keyStore", letsEncryptKeystore.getAbsolutePath())
                .setProperty("keyStorePassword", "123456")
                .setProperty("trustStore", letsEncryptKeystore.getAbsolutePath())
                .setProperty("trustStorePassword", "123456");

        Config config = smallInstanceConfig().setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
