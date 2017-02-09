package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.CipherHelper.SymmetricCipherBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.nio.CipherHelper.initBouncySecurityProvider;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class CipherHelperTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CipherHelper.class);
    }

    @Test
    public void testInitBouncySecurityProvider() {
        try {
            System.setProperty("hazelcast.security.bouncy.enabled", "true");

            initBouncySecurityProvider();
        } finally {
            System.clearProperty("hazelcast.security.bouncy.enabled");
        }
    }

    @Test(expected = RuntimeException.class)
    public void testCreateCipher_withInvalidConfiguration() {
        SymmetricEncryptionConfig config = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm("invalidAlgorithm");

        new SymmetricCipherBuilder(config).create(true);
    }
}
