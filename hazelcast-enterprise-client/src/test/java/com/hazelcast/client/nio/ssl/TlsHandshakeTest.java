package com.hazelcast.client.nio.ssl;

import static com.hazelcast.TestEnvironmentUtil.isOpenSslSupported;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.IOUtil.copy;
import static com.hazelcast.nio.IOUtil.toByteArray;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import javax.net.SocketFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;
import com.hazelcast.nio.ssl.OpenSSLEngineFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests if simultaneous TLS handshakes don't block IO.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({ QuickTest.class, ParallelTest.class })
public class TlsHandshakeTest {

    private static final String KEYSTORE_SERVER = "server.keystore";
    private static final String TRUSTSTORE_SERVER = "server.truststore";
    private static final String TRUSTSTORE_CLIENT = "client.truststore";
    private static final String TLS_1_CLIENT_HELLO = "tls1-client-hello.bin";
    private static final String TLS_11_CLIENT_HELLO = "tls11-client-hello.bin";
    private static final String TLS_12_CLIENT_HELLO = "tls12-client-hello.bin";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final ILogger logger = Logger.getLogger(TlsHandshakeTest.class);

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @Parameter
    public boolean openSsl;

    @Parameters(name = "openSsl:{0}")
    public static Object[] parameters() {
        return new Object[] { false, true };
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    @Before
    public void before() {
        assumeTrue("OpenSSL enabled but not available", !openSsl || isOpenSslSupported());
    }

    @Test
    public void testClientHello1() throws Exception {
        assertClientHelloDoesNotBlock(TLS_1_CLIENT_HELLO);
    }

    @Test
    public void testClientHello11() throws Exception {
        assertClientHelloDoesNotBlock(TLS_11_CLIENT_HELLO);
    }

    @Test
    public void testClientHello12() throws Exception {
        assertClientHelloDoesNotBlock(TLS_12_CLIENT_HELLO);
    }

    /**
     * Starts 2 members with TLS enabled and then sends 5 parallel TLS CLIENT_HELLO messages to the first member. Third member
     * and one client are started during the handshakes-in-progress. Tests verifies cluster size and possibility to work with an
     * IMap before stopping the TLS handshakes.
     */
    private void assertClientHelloDoesNotBlock(String helloReourc)
            throws IOException, InterruptedException, BrokenBarrierException {
        Config config = createMemberConfig();
        HazelcastInstance hz1 = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);
        assertClusterSize(2, hz1, hz2);
        byte[] clientHello = getResource(helloReourc);
        CyclicBarrier cbAfterWrite = new CyclicBarrier(6);
        CyclicBarrier cbBeforeClose = new CyclicBarrier(6);
        for (int i = 0; i < 5; i++) {
            SendBytesRunnable runnable = new SendBytesRunnable(SocketFactory.getDefault(), getPort(hz1), clientHello,
                    cbAfterWrite, cbBeforeClose);
            new Thread(runnable).start();
        }
        cbAfterWrite.await();
        try {
            HazelcastInstance hz3 = factory.newHazelcastInstance(config);
            assertClusterSize(3, hz1, hz2, hz3);

            HazelcastInstance client = factory.newHazelcastClient(createClientConfig());
            assertEquals(0, client.getMap("test").size());
        } finally {
            cbBeforeClose.reset();
        }
    }

    private Config createMemberConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true)
                .setFactoryClassName((openSsl ? OpenSSLEngineFactory.class : BasicSSLContextFactory.class).getName())
                .setProperty("keyStore", copyResource(KEYSTORE_SERVER).getAbsolutePath())
                .setProperty("keyStorePassword", "123456")
                .setProperty("trustStore", copyResource(TRUSTSTORE_SERVER).getAbsolutePath())
                .setProperty("trustStorePassword", "123456");

        Config config = smallInstanceConfig();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(15);
        return config;
    }

    private ClientConfig createClientConfig() throws IOException {
        SSLConfig sslConfig = new SSLConfig().setEnabled(true)
                .setFactoryClassName((openSsl ? OpenSSLEngineFactory.class : BasicSSLContextFactory.class).getName())
                .setProperty("trustStore", copyResource(TRUSTSTORE_CLIENT).getAbsolutePath())
                .setProperty("trustStorePassword", "123456");

        ClientConfig config = new ClientConfig();
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setSSLConfig(sslConfig);
        networkConfig.setConnectionTimeout(15000);
        return config;
    }

    /**
     * Copies a resource file from current package to location denoted by given {@link java.io.File} instance.
     */
    private File copyResource(String resourceName) throws IOException {
        File targetFile = new File(tempFolder.getRoot(), resourceName);
        if (!targetFile.exists()) {
            assertTrue(targetFile.createNewFile());
            logger.info("Copying test resource to file " + targetFile.getAbsolutePath());
            InputStream is = null;
            try {
                is = TlsHandshakeTest.class.getResourceAsStream(resourceName);
                copy(is, targetFile);
            } finally {
                closeResource(is);
            }
        }
        return targetFile;
    }

    private byte[] getResource(String resourceName) throws IOException {
        InputStream is = null;
        try {
            is = TlsHandshakeTest.class.getResourceAsStream(resourceName);
            return toByteArray(is);
        } finally {
            closeResource(is);
        }
    }

    private static int getPort(HazelcastInstance hz) {
        return getAddress(hz).getPort();
    }

    /**
     * Quietly attempts to close a {@link Socket}, swallowing any exception.
     *
     * @param serverSocket server socket to close. If {@code null}, no action is taken.
     */
    private static void close(Socket serverSocket) {
        if (serverSocket == null) {
            return;
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            Logger.getLogger(IOUtil.class).finest("closeResource failed", e);
        }
    }


    private static class SendBytesRunnable implements Runnable {
        private final SocketFactory socketFactory;
        private final int port;
        private final byte[] payload;
        private final CyclicBarrier cbAfterWrite;
        private final CyclicBarrier cbBeforeClose;

        public SendBytesRunnable(SocketFactory socketFactory, int port, byte[] payload, CyclicBarrier cbAfterWrite,
                CyclicBarrier cbBeforeClose) {
            super();
            this.socketFactory = socketFactory;
            this.port = port;
            this.payload = payload;
            this.cbAfterWrite = cbAfterWrite;
            this.cbBeforeClose = cbBeforeClose;
        }

        @Override
        public void run() {
            Socket socket = null;
            try {
                socket = socketFactory.createSocket("127.0.0.1", port);
                socket.getOutputStream().write(payload);
                cbAfterWrite.await();
                cbBeforeClose.await();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                close(socket);
            }
        }
    }

}
