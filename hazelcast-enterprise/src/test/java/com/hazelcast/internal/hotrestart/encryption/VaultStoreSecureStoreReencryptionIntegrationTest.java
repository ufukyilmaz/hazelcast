package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.spi.impl.securestore.impl.MockVaultServer;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;

import static com.hazelcast.spi.impl.securestore.impl.MockVaultServer.start;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class VaultStoreSecureStoreReencryptionIntegrationTest extends AbstractHotRestartReencryptionIntegrationTest {

    @Parameter(1)
    public boolean https;

    @Parameters(name = "clusterSize:{0}, https:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{1, false}, {1, true}, {5, false}, {5, true}});
    }

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    private MockVaultServer vault;

    private VaultSecureStoreConfig vaultConfig;

    @Before
    public void before() {
        vaultConfig = new VaultSecureStoreConfig("http://dummy", MockVaultServer.SECRET_PATH,
                MockVaultServer.TOKEN).setPollingInterval(1);
        vault = vaultStartCatchExceptions(vaultConfig);
        rotateKeys(initialKeys());
    }

    @After
    public void after() {
        if (vault != null) {
            vault.stop();
        }
        vault = null;
    }

    private File maybeHttps() {
        try {
            return https ? new File(tf.newFolder(), "keystore") : null;
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private MockVaultServer vaultStartCatchExceptions(VaultSecureStoreConfig vaultConfig) {
        try {
            return start(maybeHttps(), vaultConfig);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected void rotateKeys(byte[]... keys) {
        vault.clearMappings(MockVaultServer.SECRET_PATH);
        for (byte[] key : keys) {
            String base64Encoded = Base64.getEncoder().encodeToString(key);
            vault.addMappingVersion(MockVaultServer.SECRET_PATH, base64Encoded);
        }
    }

    @Override
    protected SecureStoreConfig getSecureStoreConfig() {
        return vaultConfig;
    }
}
