package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.persistence.CPPersistenceServiceImpl.ALLOW_IP_ADDRESS_CHANGE;
import static com.hazelcast.cp.persistence.CPPersistenceServiceImpl.FAVOR_OWN_PERSISTENCE_DIRECTORY;
import static com.hazelcast.cp.persistence.PersistenceTestSupport.AddressPolicy.PICK_NEW;
import static com.hazelcast.cp.persistence.PersistenceTestSupport.AddressPolicy.REUSE_EXACT;
import static com.hazelcast.cp.persistence.PersistenceTestSupport.AddressPolicy.REUSE_RANDOM;
import static java.util.Arrays.asList;
import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class PersistenceTestSupport extends HazelcastRaftTestSupport {

    public enum AddressPolicy {
        /**
         * Reuse former address for each member
         */
        REUSE_EXACT,
        /**
         * Reuse former addresses but pick a random address for each member
         */
        REUSE_RANDOM,
        /**
         * Never reuse, always pick new addresses
         */
        PICK_NEW
    }

    @Parameters(name = "policy:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{REUSE_EXACT, REUSE_RANDOM, PICK_NEW});
    }

    protected final ILogger logger = Logger.getLogger(getClass());

    @Parameter
    public AddressPolicy restartAddressPolicy;

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    protected File baseDir;

    @Before
    public void before() {
        baseDir = hotRestartFolderRule.getBaseDir();
    }

    @Override
    protected Config createConfig(int cpMemberCount, int groupSize) {
        Config config = super.createConfig(cpMemberCount, groupSize);
        config.getCPSubsystemConfig().setPersistenceEnabled(true).setBaseDir(baseDir);

        boolean allowAddressChange = true;
        if (restartAddressPolicy == AddressPolicy.REUSE_EXACT) {
            allowAddressChange = false;
        }
        config.setProperty(ALLOW_IP_ADDRESS_CHANGE.getName(), Boolean.toString(allowAddressChange));

        boolean favorOwnAddress = true;
        if (restartAddressPolicy == AddressPolicy.REUSE_RANDOM) {
            favorOwnAddress = false;
        }
        config.setProperty(FAVOR_OWN_PERSISTENCE_DIRECTORY.getName(), Boolean.toString(favorOwnAddress));
        return config;
    }

    protected void assertCommitIndexesSame(final HazelcastInstance[] instances, final RaftGroupId groupId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RaftNodeImpl node = getRaftNode(instances[0], groupId);
                assertNotNull(node);
                long referenceCommitIndex = getCommitIndex(node);

                for (HazelcastInstance instance : instances) {
                    RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                    assertNotNull(raftNode);
                    long commitIndex = getCommitIndex(raftNode);
                    assertEquals(referenceCommitIndex, commitIndex);
                }
            }
        });
    }

    protected HazelcastInstance[] restartInstances(Address[] addresses, final Config config) {
        final List<HazelcastInstance> instancesList = synchronizedList(new ArrayList<HazelcastInstance>());
        final CountDownLatch latch = new CountDownLatch(addresses.length);

        for (final Address address : addresses) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        HazelcastInstance instance = factory.newHazelcastInstance(restartingAddress(address), config);
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

    protected HazelcastInstance restartInstance(Address address, Config config) {
        return factory.newHazelcastInstance(restartingAddress(address), config);
    }

    protected Address restartingAddress(Address address) {
        return restartAddressPolicy != AddressPolicy.PICK_NEW ? address : factory.nextAddress();
    }
}
