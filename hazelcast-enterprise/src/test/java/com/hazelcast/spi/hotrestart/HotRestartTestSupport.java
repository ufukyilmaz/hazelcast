package com.hazelcast.spi.hotrestart;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cache.hotrestart.HotRestartTestUtil.createFolder;
import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static java.util.Arrays.fill;

public abstract class HotRestartTestSupport extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Rule
    public TestName testName = new TestName();

    private static InetAddress localAddress;

    protected File baseDir;

    protected void setupInternal() {

    }

    @Before
    public final void setup() {
        factory = createFactory();
        baseDir = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        createFolder(baseDir);

        setupInternal();
    }

    @After
    public final void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
        if (baseDir != null) {
            delete(baseDir);
        }
    }

    @BeforeClass
    public static void setupClass() throws Exception {
        localAddress = InetAddress.getLocalHost();
    }


    protected final HazelcastInstance newHazelcastInstance(IFunction<Address, Config> configFn) {
        Address address = factory.nextAddress();
        Config config = configFn.apply(address);
        return factory.newHazelcastInstance(address, config);
    }

    protected final HazelcastInstance restartHazelcastInstance(HazelcastInstance hz, Config config) {
        Address address = getNode(hz).getThisAddress();
        hz.shutdown();
        return factory.newHazelcastInstance(address, config);
    }

    protected final HazelcastInstance[] restartCluster(int newClusterSize, final IFunction<Address, Config> configFn) {
        if (factory != null) {
            Iterator<HazelcastInstance> iterator = factory.getAllHazelcastInstances().iterator();
            if (iterator.hasNext()) {
                iterator.next().getCluster().shutdown();
            }
            factory.terminateAll();
        }
        factory = createFactory();

        final CountDownLatch latch = new CountDownLatch(newClusterSize);
        for (int i = 0; i < newClusterSize; i++) {
            final Address address = new Address("127.0.0.1", localAddress, 5000 + i);
            spawn(new Runnable() {
                @Override
                public void run() {
                    Config config = configFn.apply(address);
                    factory.newHazelcastInstance(address, config);
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch);

        Collection<HazelcastInstance> instances = factory.getAllHazelcastInstances();
        assertClusterSizeEventually(newClusterSize, instances);
        return instances.toArray(new HazelcastInstance[0]);
    }

    protected final Collection<HazelcastInstance> getAllHazelcastInstances() {
        return factory.getAllHazelcastInstances();
    }

    protected final HazelcastInstance getFirstInstance() {
        if (factory == null) {
            throw new IllegalStateException("Instance factory is not initiated!");
        }
        Iterator<HazelcastInstance> iterator = factory.getAllHazelcastInstances().iterator();
        if (!iterator.hasNext()) {
            throw new IllegalStateException("Instance factory has no instance created!");
        }
        return iterator.next();
    }

    private TestHazelcastInstanceFactory createFactory() {
        String[] addresses = new String[10];
        fill(addresses, "127.0.0.1");
        return new TestHazelcastInstanceFactory(5000, addresses);
    }
}
