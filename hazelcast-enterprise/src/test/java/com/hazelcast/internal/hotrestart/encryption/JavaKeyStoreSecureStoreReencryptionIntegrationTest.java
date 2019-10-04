package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_PASSWORD;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_TYPE_PKCS12;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JavaKeyStoreSecureStoreReencryptionIntegrationTest extends AbstractHotRestartReencryptionIntegrationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private JavaKeyStoreSecureStoreConfig keyStoreConfig;

    @Before
    public void before() throws IOException {
        keyStoreConfig = recreateJavaKeyStore(tempFolder.newFile(), initialKeys());
        keyStoreConfig.setPollingInterval(1);
    }

    @Override
    protected void rotateKeys(byte[]... keys) {
        recreateJavaKeyStore(keyStoreConfig.getPath(), keys);
    }

    @Override
    protected SecureStoreConfig getSecureStoreConfig() {
        return keyStoreConfig;
    }

    private static JavaKeyStoreSecureStoreConfig recreateJavaKeyStore(File file, byte[]... keys) {
        return TestJavaKeyStoreSecureStoreUtils.createJavaKeyStore(file, KEYSTORE_TYPE_PKCS12, KEYSTORE_PASSWORD, keys);
    }

}
