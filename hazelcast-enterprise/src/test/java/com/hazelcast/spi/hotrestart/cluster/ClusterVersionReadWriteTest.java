package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;

/**
 * Verify cluster version read/write.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterVersionReadWriteTest extends MetadataReaderWriterTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private Version version;
    private ILogger logger = Logger.getLogger(ClusterVersionReadWriteTest.class);

    @Override
    void setupInternal() {
        version = Version.of(BuildInfoProvider.getBuildInfo().getVersion());
    }

    @Test
    public void test_readNotExistingFolder() throws Exception {
        assertEquals(Version.UNKNOWN, ClusterVersionReader.readClusterVersion(getNonExistingFolder()));
    }

    @Test
    public void test_readEmptyFolder() throws Exception {
        assertEquals(Version.UNKNOWN, ClusterVersionReader.readClusterVersion(folder));
    }

    @Test
    public void test_writeNotExistingFolder() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(getNonExistingFolder());
        expectedException.expect(FileNotFoundException.class);
        writer.write(version);
    }

    @Test
    public void test_unknownVersionWriteRead() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(folder);
        writer.write(Version.UNKNOWN);

        assertEquals(Version.UNKNOWN, ClusterVersionReader.readClusterVersion(folder));
    }

    @Test
    public void test_WriteRead() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(folder);
        writer.write(version);

        assertEquals(version, ClusterVersionReader.readClusterVersion(folder));
    }
}
