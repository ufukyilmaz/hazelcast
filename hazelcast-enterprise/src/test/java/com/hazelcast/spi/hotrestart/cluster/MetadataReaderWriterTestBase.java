package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.UuidUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.net.InetAddress;
import java.util.Random;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;

public abstract class MetadataReaderWriterTestBase extends HazelcastTestSupport {

    @Rule
    public final TestName testName = new TestName();

    protected InetAddress localAddress;
    protected File folder;

    @Before
    public final void setup() throws Exception {
        localAddress = InetAddress.getLocalHost();
        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
        setupInternal();
    }

    void setupInternal() {
    }

    @After
    public final void tearDown() {
        if (folder != null) {
            delete(folder);
        }

        tearDownInternal();
    }

    void tearDownInternal() {
    }

    final PartitionReplica[] initializeReplicas(int len) {
        PartitionReplica[] addresses = new PartitionReplica[len];
        Random random = new Random();
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = new PartitionReplica(new Address("10.10.10." + random.nextInt(256), localAddress, i + 1), UuidUtil.newUnsecureUuidString());
        }
        return addresses;
    }

    final File getNonExistingFolder() {
        return new File(folder.getParentFile(), "I-dont-exist");
    }
}
