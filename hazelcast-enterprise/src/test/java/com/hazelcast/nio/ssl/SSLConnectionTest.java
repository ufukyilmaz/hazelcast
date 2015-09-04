package com.hazelcast.nio.ssl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.tcp.SocketChannelWrapper;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SSLConnectionTest {

    private static final int PORT = 13131;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 1000 * 60)
    public void testSockets() throws Exception {
        ServerSocketChannel serverSocketChannel = null;
        Socket socket = null;
        final ExecutorService ex = Executors.newCachedThreadPool();
        try {
            serverSocketChannel = openAndBindServerSocketChannel();

            int count = 250;
            ex.execute(new ServerSocketChannelProcessor(serverSocketChannel, count, ex));

            SSLContext clientContext = createClientSslContext();
            javax.net.ssl.SSLSocketFactory socketFactory = clientContext.getSocketFactory();
            socket = socketFactory.createSocket();
            socket.connect(new InetSocketAddress(PORT));

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            for (int i = 0; i < count; i++) {
                out.writeInt(i);
                out.flush();
                int k = in.readInt();
                assertEquals(i * 2 + 1, k);
            }
        } finally {
            ex.shutdownNow();
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                }
            }
            IOUtil.closeResource(serverSocketChannel);
        }
    }

    @Test(timeout = 1000 * 60)
    public void testSocketChannels() throws Exception {
        ServerSocketChannel serverSocketChannel = null;
        SocketChannelWrapper socketChannel = null;
        final ExecutorService ex = Executors.newCachedThreadPool();
        try {
            serverSocketChannel = openAndBindServerSocketChannel();

            int count = 1000;
            ex.execute(new ServerSocketChannelProcessor(serverSocketChannel, count, ex));

            final AtomicReference<Error> error = new AtomicReference<Error>();
            SSLContext clientContext = createClientSslContext();
            socketChannel = new SSLSocketChannelWrapper(clientContext, SocketChannel.open(), true);
            socketChannel.connect(new InetSocketAddress(PORT));
            final CountDownLatch latch = new CountDownLatch(2);

            ex.execute(new ChannelWriter(socketChannel, count, latch) {
                int prepareData(int i) throws Exception {
                    return i;
                }
            });

            ex.execute(new ChannelReader(socketChannel, count, latch) {
                void processData(int i, int data) throws Exception {
                    try {
                        assertEquals(i * 2 + 1, data);
                    } catch (AssertionError e) {
                        error.compareAndSet(null, e);
                        throw e;
                    }
                }
            });

            latch.await(2, TimeUnit.MINUTES);

            Error e = error.get();
            if (e != null) {
                throw e;
            }
        } finally {
            ex.shutdownNow();
            IOUtil.closeResource(socketChannel);
            IOUtil.closeResource(serverSocketChannel);
        }
    }

    private ServerSocketChannel openAndBindServerSocketChannel() throws IOException {
        ServerSocketChannel serverSocketChannel;
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(true);
        serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
        return serverSocketChannel;
    }

    private abstract class ChannelReader implements Runnable {
        final int count;
        final SocketChannelWrapper socketChannel;
        final CountDownLatch latch;

        private ChannelReader(SocketChannelWrapper socketChannel, int count, CountDownLatch latch) {
            this.socketChannel = socketChannel;
            this.count = count;
            this.latch = latch;
        }

        public void run() {
            ByteBuffer in = ByteBuffer.allocate(4);
            try {
                for (int i = 0; i < count; i++) {
                    while (in.hasRemaining()) {
                        socketChannel.read(in);
                    }
                    in.flip();
                    int read = in.getInt();
                    processData(i, read);
                    in.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        abstract void processData(int i, int data) throws Exception;
    }

    private abstract class ChannelWriter implements Runnable {
        final int count;
        final SocketChannelWrapper socketChannel;
        final CountDownLatch latch;

        private ChannelWriter(SocketChannelWrapper socketChannel, int count, CountDownLatch latch) {
            this.socketChannel = socketChannel;
            this.count = count;
            this.latch = latch;
        }

        public final void run() {
            ByteBuffer out = ByteBuffer.allocate(4);
            try {
                for (int i = 0; i < count; i++) {
                    int data = prepareData(i);
                    out.putInt(data);
                    out.flip();
                    while (out.hasRemaining()) {
                        socketChannel.write(out);
                    }
                    out.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        abstract int prepareData(int i) throws Exception;
    }

    private class ServerSocketChannelProcessor implements Runnable {
        private final ServerSocketChannel ssc;
        private final int count;
        private final ExecutorService ex;

        public ServerSocketChannelProcessor(ServerSocketChannel ssc, int count,
                ExecutorService ex) {
            this.ssc = ssc;
            this.count = count;
            this.ex = ex;
        }

        public void run() {
            SocketChannelWrapper socketChannel = null;
            try {
                SSLContext context = createServerSslContext();
                socketChannel = new SSLSocketChannelWrapper(context, ssc.accept(), false);
                final CountDownLatch latch = new CountDownLatch(2);
                final BlockingQueue<Integer> queue = new ArrayBlockingQueue<Integer>(count);

                ex.execute(new ChannelReader(socketChannel, count, latch) {
                    void processData(int i, int data) throws Exception {
                        queue.add(data);
                    }
                });
                ex.execute(new ChannelWriter(socketChannel, count, latch) {
                    int prepareData(int i) throws Exception {
                        int data = queue.poll(30, TimeUnit.SECONDS);
                        return data * 2 + 1;
                    }
                });

                latch.await(2, TimeUnit.MINUTES);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                IOUtil.closeResource(socketChannel);
            }
        }
    }

    private static SSLContext createServerSslContext() throws Exception {
        SSLContextFactory factory = new BasicSSLContextFactory();
        Properties props = TestKeyStoreUtil.createSslProperties();
        factory.init(props);
        return factory.getSSLContext();
    }

    private static SSLContext createClientSslContext() throws Exception {
        SSLContextFactory factory = new BasicSSLContextFactory();
        Properties props = TestKeyStoreUtil.createSslProperties();
        // no need for keystore on client side
        props.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE);
        props.remove(TestKeyStoreUtil.JAVAX_NET_SSL_KEY_STORE_PASSWORD);
        factory.init(props);
        return factory.getSSLContext();
    }

    @Test(timeout = 1000 * 180)
    public void testNodes() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT, "1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1").setConnectionTimeoutSeconds(3000);

        Properties props = TestKeyStoreUtil.createSslProperties();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());

        TestUtil.warmUpPartitions(h1, h2, h3);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        Member owner3 = h3.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);
        assertEquals(owner1, owner3);

        String name = "ssl-test";
        int count = 128;
        IMap<Integer, byte[]> map1 = h1.getMap(name);
        for (int i = 1; i < count; i++) {
            map1.put(i, new byte[1024 * i]);
        }

        IMap<Integer, byte[]> map2 = h2.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map2.get(i);
            assertEquals(i * 1024, bytes.length);
        }

        IMap<Integer, byte[]> map3 = h3.getMap(name);
        for (int i = 1; i < count; i++) {
            byte[] bytes = map3.get(i);
            assertEquals(i * 1024, bytes.length);
        }
    }

    @Test(timeout = 1000 * 600)
    public void testPutAndGetAlwaysGoesToWire() throws Exception {
        Config config = new Config();
        config.setProperty(GroupProperty.IO_THREAD_COUNT, "1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1").setConnectionTimeoutSeconds(3000);

        Properties props = TestKeyStoreUtil.createSslProperties();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(true).setProperties(props));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        TestUtil.warmUpPartitions(h1, h2);
        Member owner1 = h1.getPartitionService().getPartition(0).getOwner();
        Member owner2 = h2.getPartitionService().getPartition(0).getOwner();
        assertEquals(owner1, owner2);

        String name = "ssl-test";

        IMap<String, byte[]> map1 = h1.getMap(name);
        final int count = 256;
        for (int i = 1; i <= count; i++) {
            final String key = HazelcastTestSupport.generateKeyOwnedBy(h2);
            map1.put(key, new byte[1024 * i]);
            byte[] bytes = map1.get(key);
            assertEquals(i * 1024, bytes.length);

        }
        assertEquals(count, map1.size());
    }
}
