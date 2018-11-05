package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.TestEnvironmentUtil.copyTestResource;
import static com.hazelcast.TestEnvironmentUtil.isIbmJvm;
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
        sslConfig.setFactoryImplementation(new OpenSSLEngineFactory())
                .setProperty("keyFile",
                        copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "privkey.pem").getAbsolutePath())
                .setProperty("keyCertChainFile",
                        copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "fullchain.pem").getAbsolutePath())
                .setProperty("trustCertCollectionFile",
                        copyTestResource(SSLConnectionTest.class, tempFolder.getRoot(), "chain.pem").getAbsolutePath());

        Config config = new Config().setProperty(GroupProperty.IO_THREAD_COUNT.getName(), "1");
        config.getNetworkConfig().setSSLConfig(sslConfig);
        return config;
    }
}
