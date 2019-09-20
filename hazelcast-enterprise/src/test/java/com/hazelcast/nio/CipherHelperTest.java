package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.CipherHelper.SymmetricCipherBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;

import static com.hazelcast.nio.CipherHelper.createSymmetricReaderCipher;
import static com.hazelcast.nio.CipherHelper.createSymmetricWriterCipher;
import static com.hazelcast.nio.CipherHelper.initBouncySecurityProvider;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class CipherHelperTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SymmetricEncryptionConfig invalidConfiguration;
    private Connection mockedConnection;

    @Before
    public void setUp() {
        invalidConfiguration = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm("invalidAlgorithm");

        mockedConnection = mock(Connection.class);
    }

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

    @Test
    public void testCreateCipher_withInvalidConfiguration() {
        SymmetricCipherBuilder builder = new SymmetricCipherBuilder(invalidConfiguration);

        expectedException.expect(new RootCauseMatcher(NoSuchAlgorithmException.class));
        builder.create(true);
    }

    @Test
    public void testCreateReaderCipher_withInvalidConfiguration_expectConnectionClose() {
        try {
            Cipher cipher = createSymmetricReaderCipher(invalidConfiguration, mockedConnection);
            fail("Cipher creation should have failed! -> Algorithm: " + (cipher != null ? cipher.getAlgorithm() : "<none>"));
        } catch (Exception e) {
            verifyConnectionIsClosed(mockedConnection);
        }
    }

    @Test
    public void testCreateWriterCipher_withInvalidConfiguration_expectConnectionClose() {
        try {
            Cipher cipher = createSymmetricWriterCipher(invalidConfiguration, mockedConnection);
            fail("Cipher creation should have failed! -> Algorithm: " + (cipher != null ? cipher.getAlgorithm() : "<none>"));
        } catch (Exception e) {
            verifyConnectionIsClosed(mockedConnection);
        }
    }

    private static void verifyConnectionIsClosed(Connection connection) {
        verify(connection, times(1)).close(isNull(String.class), any(Exception.class));
    }
}
