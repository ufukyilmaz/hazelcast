package com.hazelcast.internal.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.UuidUtil;
import org.junit.Before;
import org.junit.Rule;

import java.io.File;
import java.net.InetAddress;
import java.util.Random;

public abstract class MetadataReaderWriterTestBase extends HazelcastTestSupport {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    protected InetAddress localAddress;
    protected File folder;

    @Before
    public final void setup() throws Exception {
        localAddress = InetAddress.getLocalHost();
        folder = hotRestartFolderRule.getBaseDir();
        setupInternal();
    }

    void setupInternal() {
    }

    final PartitionReplica[] initializeReplicas(int len) {
        PartitionReplica[] addresses = new PartitionReplica[len];
        Random random = new Random();
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = new PartitionReplica(new Address("10.10.10." + random.nextInt(256), localAddress, i + 1), UuidUtil.newUnsecureUUID());
        }
        return addresses;
    }

    final File getNonExistingFolder() {
        return new File(folder.getParentFile(), "I-dont-exist");
    }
}
