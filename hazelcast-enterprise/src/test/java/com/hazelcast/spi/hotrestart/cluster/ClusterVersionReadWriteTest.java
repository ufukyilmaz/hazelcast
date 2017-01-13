/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.ClusterVersion;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Verify cluster version read/write.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionReadWriteTest extends MetadataReaderWriterTestBase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ClusterVersion version;
    private ILogger logger = Logger.getLogger(ClusterVersionReadWriteTest.class);

    @Override
    void setupInternal() {
        version = ClusterVersion.of(BuildInfoProvider.BUILD_INFO.getVersion());
    }

    @Test
    public void test_readNotExistingFolder() throws Exception {
        assertNull(ClusterVersionReader.readClusterVersion(logger, getNonExistingFolder()));
    }

    @Test
    public void test_readEmptyFolder() throws Exception {
        assertNull(ClusterVersionReader.readClusterVersion(logger, folder));
    }

    @Test
    public void test_writeNotExistingFolder() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(getNonExistingFolder());
        expectedException.expect(FileNotFoundException.class);
        writer.write(version);
    }

    @Test
    public void test_NullWriteRead() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(folder);
        writer.write(null);

        assertNull(ClusterVersionReader.readClusterVersion(logger, folder));
    }

    @Test
    public void test_WriteRead() throws Exception {
        ClusterVersionWriter writer = new ClusterVersionWriter(folder);
        writer.write(version);

        assertEquals(version, ClusterVersionReader.readClusterVersion(logger, folder));
    }
}
