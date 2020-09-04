package com.hazelcast.internal.hotrestart;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public abstract class HotRestartTestSupport extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    protected File baseDir;

    protected void setupInternal() {

    }

    @Before
    public final void setup() {
        factory = createHazelcastInstanceFactory();
        baseDir = hotRestartFolderRule.getBaseDir();
        setupInternal();
    }

    protected final HazelcastInstance newHazelcastInstance(Supplier<Config> configFn) {
        Config config = configFn.get();
        return factory.newHazelcastInstance(config);
    }

    protected final HazelcastInstance restartHazelcastInstance(HazelcastInstance hz, Config config) {
        Address address = getNode(hz).getThisAddress();
        hz.shutdown();
        return factory.newHazelcastInstance(address, config);
    }

    protected final HazelcastInstance[] restartCluster(int newClusterSize, final Supplier<Config> configFn) {
        if (factory != null) {
            Iterator<HazelcastInstance> iterator = factory.getAllHazelcastInstances().iterator();
            if (iterator.hasNext()) {
                iterator.next().getCluster().shutdown();
            }
            factory.terminateAll();
        }

        Future[] futures = new Future[newClusterSize];
        for (int i = 0; i < newClusterSize; i++) {
            futures[i] = spawn(new Runnable() {
                @Override
                public void run() {
                    Config config = configFn.get();
                    factory.newHazelcastInstance(config);
                }
            });
        }
        // Future.get() uncovers failures (ExecutionExcepion)
        for (int i = 0; i < newClusterSize; i++) {
            try {
                futures[i].get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            } catch (Exception e) {
                Logger.getLogger(getClass()).severe(e);
                fail("Cluster restart failed! " + e);
            }
        }

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
}
