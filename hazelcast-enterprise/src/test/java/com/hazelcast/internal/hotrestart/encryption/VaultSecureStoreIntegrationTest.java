package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.VaultSecureStoreConfig;
import com.hazelcast.spi.impl.securestore.impl.MockVaultServer;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
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
import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.spi.impl.securestore.impl.MockVaultServer.start;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class})
public class VaultSecureStoreIntegrationTest extends AbstractHotRestartSecureStoreIntegrationTest {

    @Parameter
    public boolean https;

    @Parameters(name = "https:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(false, true);
    }

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    private MockVaultServer vault;

    @After
    public void tearDown() {
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
    protected VaultSecureStoreConfig createSecureStoreConfig() {
        VaultSecureStoreConfig vaultConfig = new VaultSecureStoreConfig("http://dummy", MockVaultServer.SECRET_PATH,
                MockVaultServer.TOKEN);
        vault = vaultStartCatchExceptions(vaultConfig);
        return vaultConfig;
    }

    @Override
    protected VaultSecureStoreConfig createSecureStoreNotAccessibleConfig() {
        return new VaultSecureStoreConfig(https ? "https://localhost:9999" : "http://localhost:9999", "foo", "bar");
    }

    @Override
    protected VaultSecureStoreConfig createSecureStoreNoKeysConfig() {
        VaultSecureStoreConfig vaultConfig = new VaultSecureStoreConfig("http://dummy", MockVaultServer.SECRET_PATH_WITHOUT_SECRET,
                MockVaultServer.TOKEN);
        vault = vaultStartCatchExceptions(vaultConfig);
        return vaultConfig;
    }

}
