package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SocketInterceptorTest extends HazelcastTestSupport {

    @After
    public void killAllHazelcastInstances() {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testSuccessfulSocketInterceptor() {
        Config config = new Config();
        config.getSecurityConfig().setEnabled(true);
        SocketInterceptorConfig socketInterceptorConfig = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(true);
        socketInterceptorConfig.setImplementation(mySocketInterceptor).setEnabled(true);
        config.getNetworkConfig().setSocketInterceptorConfig(socketInterceptorConfig);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        warmUpPartitions(h1, h2);

        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h1.getCluster().getMembers().size());

        assertEquals(1, mySocketInterceptor.getAcceptCallCount());
        assertEquals(1, mySocketInterceptor.getConnectCallCount());
        assertEquals(2, mySocketInterceptor.getInitCallCount());
        assertEquals(0, mySocketInterceptor.getAcceptFailureCount());
        assertEquals(0, mySocketInterceptor.getConnectFailureCount());
    }

    @Test(timeout = 120000)
    public void testFailingSocketInterceptor() {
        Config config = new Config();
        SocketInterceptorConfig sic = new SocketInterceptorConfig();
        MySocketInterceptor mySocketInterceptor = new MySocketInterceptor(false);
        sic.setImplementation(mySocketInterceptor).setEnabled(true);
        config.getNetworkConfig().setSocketInterceptorConfig(sic);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        assertEquals(1, h2.getCluster().getMembers().size());
        assertEquals(1, h1.getCluster().getMembers().size());
    }

    public static class MySocketInterceptor implements MemberSocketInterceptor {

        final AtomicInteger initCallCount = new AtomicInteger();
        final AtomicInteger acceptCallCount = new AtomicInteger();
        final AtomicInteger connectCallCount = new AtomicInteger();
        final AtomicInteger acceptFailureCount = new AtomicInteger();
        final AtomicInteger connectFailureCount = new AtomicInteger();
        final boolean successful;

        @Override
        public void init(Properties properties) {
            initCallCount.incrementAndGet();
        }

        public MySocketInterceptor(boolean successful) {
            this.successful = successful;
        }

        @Override
        public void onAccept(Socket acceptedSocket) throws IOException {
            acceptCallCount.incrementAndGet();
            try {
                OutputStream out = acceptedSocket.getOutputStream();
                InputStream in = acceptedSocket.getInputStream();
                int loop = new Random().nextInt(2) + 1;
                int secretValue = 1;
                int expected = (int) Math.pow(2, loop);
                for (int i = 0; i < loop; i++) {
                    out.write(secretValue);
                    int read = in.read();
                    if (read != 2 * secretValue) {
                        throw new IOException("Authentication Failed");
                    }
                    secretValue = read;
                }
                if (secretValue != expected) {
                    throw new IOException("Authentication Failed");
                }
                out.write(0);
            } catch (IOException e) {
                acceptFailureCount.incrementAndGet();
                throw e;
            }
        }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException {
            connectCallCount.incrementAndGet();
            try {
                OutputStream out = connectedSocket.getOutputStream();
                InputStream in = connectedSocket.getInputStream();
                int multiplyBy = (successful) ? 2 : 1;
                while (true) {
                    int read = in.read();
                    if (read == 0) {
                        return;
                    }
                    out.write(read * multiplyBy);
                    out.flush();
                }
            } catch (IOException e) {
                connectFailureCount.incrementAndGet();
                throw e;
            }
        }

        public int getInitCallCount() {
            return initCallCount.get();
        }

        public int getAcceptCallCount() {
            return acceptCallCount.get();
        }

        public int getConnectCallCount() {
            return connectCallCount.get();
        }

        public int getAcceptFailureCount() {
            return acceptFailureCount.get();
        }

        public int getConnectFailureCount() {
            return connectFailureCount.get();
        }

        @Override
        public String toString() {
            return "MySocketInterceptor{" +
                    "initCallCount=" + initCallCount +
                    ", acceptCallCount=" + acceptCallCount +
                    ", connectCallCount=" + connectCallCount +
                    ", acceptFailureCount=" + acceptFailureCount +
                    ", connectFailureCount=" + connectFailureCount +
                    '}';
        }
    }
}
