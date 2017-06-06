package com.hazelcast.nio;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.CipherHelper.SymmetricCipherBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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

    private SymmetricEncryptionConfig invalidConfiguration;
    private Connection mockedConnection;

    @Before
    public void setUp() {
        invalidConfiguration = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm("invalidAlgorithm");

        mockedConnection = mock(Connection.class);
    }

    @After
    public void tearDown() {
        CipherHelper.reset();
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

    @Test(expected = RuntimeException.class)
    public void testCreateCipher_withInvalidConfiguration() {
        new SymmetricCipherBuilder(invalidConfiguration).create(true);
    }

    @Test
    public void testCreateReaderCipher_withInvalidConfiguration_expectConnectionClose() {
        try {
            createSymmetricReaderCipher(invalidConfiguration, mockedConnection);
            fail("Expected an exception to be thrown!");
        } catch (RuntimeException ex) {
            verifyConnectionIsClosed(mockedConnection);
        }
    }

    @Test
    public void testCreateWriterCipher_withInvalidConfiguration_expectConnectionClose() {
        try {
            createSymmetricWriterCipher(invalidConfiguration, mockedConnection);
            fail("Expected an exception to be thrown!");
        } catch (RuntimeException ex) {
            verifyConnectionIsClosed(mockedConnection);
        }
    }

    private static void verifyConnectionIsClosed(Connection connection) {
        verify(connection, times(1)).close(isNull(String.class), any(Exception.class));
    }
}
