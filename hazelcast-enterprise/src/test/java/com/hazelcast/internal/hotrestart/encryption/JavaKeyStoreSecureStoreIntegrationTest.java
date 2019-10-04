package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_PASSWORD;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEY_BYTES;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.createJavaKeyStore;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JavaKeyStoreSecureStoreIntegrationTest extends AbstractHotRestartSecureStoreIntegrationTest {

    @Parameters(name = "keyStoreType:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{"PKCS12", "JCEKS"});
    }

    @Parameterized.Parameter
    public String keyStoreType;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    private File newTempFileSafe() {
        try {
            return tf.newFile();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected JavaKeyStoreSecureStoreConfig createSecureStoreConfig() {
        return createJavaKeyStore(newTempFileSafe(), keyStoreType, KEYSTORE_PASSWORD, KEY_BYTES);
    }

    @Override
    protected JavaKeyStoreSecureStoreConfig createSecureStoreNotAccessibleConfig() {
        return new JavaKeyStoreSecureStoreConfig(new File("I-do-not-exist")).setType(keyStoreType);
    }

    @Override
    protected JavaKeyStoreSecureStoreConfig createSecureStoreUnusableKeysConfig() {
        return createJavaKeyStore(newTempFileSafe(), keyStoreType, KEYSTORE_PASSWORD, new byte[1]);
    }

    @Override
    protected JavaKeyStoreSecureStoreConfig createSecureStoreNoKeysConfig() {
        return createJavaKeyStore(newTempFileSafe(), keyStoreType, KEYSTORE_PASSWORD);
    }

}
