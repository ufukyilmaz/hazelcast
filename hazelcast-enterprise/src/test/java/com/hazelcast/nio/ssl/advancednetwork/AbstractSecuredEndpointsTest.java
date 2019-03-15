package com.hazelcast.nio.ssl.advancednetwork;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ssl.SSLEngineFactorySupport;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Properties;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_MUTUAL_AUTHENTICATION;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE;
import static com.hazelcast.nio.ssl.TestKeyStoreUtil.JAVAX_NET_SSL_TRUST_STORE_PASSWORD;
import static org.junit.Assert.fail;

public class AbstractSecuredEndpointsTest {

    protected static final int MEMBER_PORT = 11000;
    protected static final int CLIENT_PORT = MEMBER_PORT + 1;
    protected static final int WAN_PORT = MEMBER_PORT + 2;
    protected static final int REST_PORT = MEMBER_PORT + 3;
    protected static final int MEMCACHE_PORT = MEMBER_PORT + 4;

    protected static final String MEMBER_A_KEYSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberA.keystore";
    protected static final String MEMBER_A_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberA.truststore";
    protected static final String MEMBER_B_KEYSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberB.keystore";
    protected static final String MEMBER_B_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberB.truststore";
    protected static final String MEMBER_FOR_CLIENT_KEYSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberForClient.keystore";
    protected static final String CLIENT_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/client.truststore";
    protected static final String MEMBER_FOR_WAN_KEYSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberForWan.keystore";
    protected static final String WAN_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/wan.truststore";
    protected static final String MEMBER_FOR_REST_KEYSTORE = "com/hazelcast/nio/ssl/advancednetwork/memberForRest.keystore";
    protected static final String REST_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/rest.truststore";
    protected static final String MEMBER_FOR_MEMCACHE_KEYSTORE
            = "com/hazelcast/nio/ssl/advancednetwork/memberForMemcache.keystore";
    protected static final String MEMCACHE_TRUSTSTORE = "com/hazelcast/nio/ssl/advancednetwork/memcache.truststore";

    protected static final String MEMBER_PASSWORD = "123456member";
    protected static final String CLIENT_PASSWORD = "123456client";
    protected static final String WAN_PASSWORD = "123456wan";
    protected static final String REST_PASSWORD = "123456rest";
    protected static final String MEMCACHE_PASSWORD = "123456memcache";

    protected static File memberAKeystore;
    protected static File memberATruststore;
    protected static File memberBKeystore;
    protected static File memberBTruststore;
    protected static File memberForClientKeystore;
    protected static File clientTruststore;
    protected static File memberForWanKeystore;
    protected static File wanTruststore;
    protected static File memberForRestKeystore;
    protected static File restTruststore;
    protected static File memberForMemcacheKeystore;
    protected static File memcacheTruststore;

    protected HazelcastInstance hz;

    @BeforeClass
    public static void prepareKeystores() {
        memberAKeystore = TestKeyStoreUtil.createTempFile(MEMBER_A_KEYSTORE);
        memberATruststore = TestKeyStoreUtil.createTempFile(MEMBER_A_TRUSTSTORE);
        memberBKeystore = TestKeyStoreUtil.createTempFile(MEMBER_B_KEYSTORE);
        memberBTruststore = TestKeyStoreUtil.createTempFile(MEMBER_B_TRUSTSTORE);
        memberForClientKeystore = TestKeyStoreUtil.createTempFile(MEMBER_FOR_CLIENT_KEYSTORE);
        clientTruststore = TestKeyStoreUtil.createTempFile(CLIENT_TRUSTSTORE);

        memberForWanKeystore = TestKeyStoreUtil.createTempFile(MEMBER_FOR_WAN_KEYSTORE);
        wanTruststore = TestKeyStoreUtil.createTempFile(WAN_TRUSTSTORE);
        memberForRestKeystore = TestKeyStoreUtil.createTempFile(MEMBER_FOR_REST_KEYSTORE);
        restTruststore = TestKeyStoreUtil.createTempFile(REST_TRUSTSTORE);
        memberForMemcacheKeystore = TestKeyStoreUtil.createTempFile(MEMBER_FOR_MEMCACHE_KEYSTORE);
        memcacheTruststore = TestKeyStoreUtil.createTempFile(MEMCACHE_TRUSTSTORE);
    }

    @AfterClass
    public static void removeKeystores() {
        IOUtil.deleteQuietly(memberAKeystore);
        IOUtil.deleteQuietly(memberATruststore);
        IOUtil.deleteQuietly(memberBKeystore);
        IOUtil.deleteQuietly(memberBTruststore);
        IOUtil.deleteQuietly(memberForClientKeystore);
        IOUtil.deleteQuietly(clientTruststore);
        IOUtil.deleteQuietly(memberForWanKeystore);
        IOUtil.deleteQuietly(wanTruststore);
        IOUtil.deleteQuietly(memberForRestKeystore);
        IOUtil.deleteQuietly(restTruststore);
        IOUtil.deleteQuietly(memberForMemcacheKeystore);
        IOUtil.deleteQuietly(memcacheTruststore);
    }

    @After
    public void tearDown() {
        if (hz != null) {
            hz.getLifecycleService().terminate();
        }
    }

    protected Properties prepareSslProperties(File keystore, String password) {
        return prepareSslPropertiesWithTrustStore(keystore, password, null, null);
    }

    protected Properties prepareSslPropertiesWithTrustStore(File keystore, String keystorePassword, File truststore,
            String truststorePassword) {
        Properties props = new Properties();
        props.setProperty(JAVAX_NET_SSL_KEY_STORE, keystore.getAbsolutePath());
        props.setProperty(JAVAX_NET_SSL_KEY_STORE_PASSWORD, keystorePassword);
        if (truststore != null && truststorePassword != null) {
            props.setProperty(JAVAX_NET_SSL_TRUST_STORE, truststore.getAbsolutePath());
            props.setProperty(JAVAX_NET_SSL_TRUST_STORE_PASSWORD, truststorePassword);
            props.setProperty(JAVAX_NET_SSL_MUTUAL_AUTHENTICATION, "REQUIRED");
        }
        return props;
    }

    protected void configureTcpIpConfig(Config config) {
        JoinConfig join = config.getAdvancedNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1:" + (MEMBER_PORT + 10)).setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
    }

    protected void testTextEndpoint(int port, File trustStore, String trustStorePassword, boolean expectPass) throws Exception {
        SSLContext sslContext;
        try {
            sslContext = SSLContext.getInstance("TLSv1.1");
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }

        TrustManagerFactory tmf = SSLEngineFactorySupport.loadTrustManagerFactory(trustStorePassword,
                trustStore.getAbsolutePath(), TrustManagerFactory.getDefaultAlgorithm());
        sslContext.init(null, tmf.getTrustManagers(), new SecureRandom());

        SSLSocketFactory socketFactory = sslContext.getSocketFactory();
        Socket createSocket = null;
        try {
            createSocket = socketFactory.createSocket("127.0.0.1", port);
            OutputStream outputStream = null;
            try {
                outputStream = createSocket.getOutputStream();
                outputStream.write(60);
                if (!expectPass) {
                    fail("SSLHandshakeException should be thrown.");
                }
            } catch (SSLHandshakeException ex) {
                if (expectPass) {
                    throw ex;
                } else {
                    // expected
                }
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }
        } finally {
            if (createSocket != null) {
                createSocket.close();
            }
        }
    }
}
