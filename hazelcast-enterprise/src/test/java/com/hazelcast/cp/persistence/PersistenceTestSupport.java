package com.hazelcast.cp.persistence;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.AssertTask;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;

public abstract class PersistenceTestSupport extends HazelcastRaftTestSupport {

    protected final ILogger logger = Logger.getLogger(getClass());

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    private File baseDir;

    @Before
    public void before() {
        baseDir = hotRestartFolderRule.getBaseDir();
    }

    @Override
    protected Config createConfig(int cpMemberCount, int groupSize) {
        Config config = super.createConfig(cpMemberCount, groupSize);
        config.getCPSubsystemConfig().setPersistenceEnabled(true).setBaseDir(baseDir);
        return config;
    }

    protected void assertCommitIndexesSame(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl node = getRaftNode(instances[2], groupId);
                long referenceCommitIndex = getCommitIndex(node);

                for (HazelcastInstance instance : instances) {
                    long commitIndex = getCommitIndex(getRaftNode(instance, groupId));
                    assertEquals(referenceCommitIndex, commitIndex);
                }
            }
        });
    }

    protected HazelcastInstance[] restartInstancesParallel(Address[] addresses, final Config config) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(addresses.length);

        for (final Address address : addresses) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = factory.newHazelcastInstance(address, config);
                        instancesList.add(instance);
                    } catch (Throwable e) {
                        logger.severe(e);
                    } finally {
                        latch.countDown();
                    }

                }
            }, "Restart thread for " + address).start();
        }

        assertOpenEventually(latch);

        return instancesList.toArray(new HazelcastInstance[0]);
    }

    protected HazelcastInstance[] restartInstances(Address[] addresses, Config config) {
        HazelcastInstance[] instances = new HazelcastInstance[addresses.length];
        for (int i = 0; i < addresses.length; i++) {
            instances[i] = factory.newHazelcastInstance(addresses[i], config);
        }
        return instances;
    }
}
