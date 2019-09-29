package com.hazelcast.internal.hotrestart.impl.io;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.hotrestart.backup.AbstractHotRestartBackupTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CopyStrategyTest extends AbstractHotRestartBackupTest {

    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();

    @Test
    public void testCopyStrategies() throws Exception {
        testStrategy(new DefaultCopyStrategy());

        final HardLinkCopyStrategy strategy = attemptGetHardLinkCopyStrategy();
        if (strategy != null) {
            testStrategy(strategy);
        }
    }

    private static HardLinkCopyStrategy attemptGetHardLinkCopyStrategy() {
        try {
            return new HardLinkCopyStrategy();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private void testStrategy(FileCopyStrategy strategy) throws Exception {
        final File source = tempDir.newFile();
        final File dest = new File(tempDir.newFolder(), "dest");
        assertFalse(dest.exists());
        strategy.copy(source, dest);
        assertTrue(dest.exists());
    }
}
