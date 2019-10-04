package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;

import java.io.IOException;

import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_PASSWORD;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEYSTORE_TYPE_PKCS12;
import static com.hazelcast.spi.impl.securestore.impl.TestJavaKeyStoreSecureStoreUtils.KEY_BYTES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultSecureStoreFactoryTest {

    private static final ILogger LOGGER = Logger.getLogger(VaultSecureStoreTest.class);

    @Rule
    public TemporaryFolder tf = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testJavaKeyStore() throws IOException {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        ExecutionService executionService = mock(ExecutionService.class);
        Node node = mock(Node.class);
        when(node.getLogger(ArgumentMatchers.any(Class.class))).thenReturn(LOGGER);
        when(node.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        JavaKeyStoreSecureStoreConfig config = TestJavaKeyStoreSecureStoreUtils
                .createJavaKeyStore(tf.newFile("keystore.p12"), KEYSTORE_TYPE_PKCS12, KEYSTORE_PASSWORD, KEY_BYTES);
        assertTrue(new DefaultSecureStoreFactory(node).getSecureStore(config) instanceof JavaKeyStoreSecureStore);
    }

    @Test
    public void testUnsupportedSecureStore() {
        Node node = mock(Node.class);
        when(node.getLogger(ArgumentMatchers.any(Class.class))).thenReturn(LOGGER);
        expectedException.expect(InvalidConfigurationException.class);
        expectedException.expectMessage(containsString("Unsupported Secure Store"));
        new DefaultSecureStoreFactory(node).getSecureStore(new SecureStoreConfig() {
        });
    }
}
