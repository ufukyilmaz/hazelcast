package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.hotrestart.backup.AbstractHotRestartBackupTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CopyStrategyTest extends AbstractHotRestartBackupTest {
    @Test
    public void testCopyStrategies() throws IOException {
        testStrategy(new DefaultCopyStrategy());

        final HardLinkCopyStrategy strategy = attemptGetHardLinkCopyStrategy();
        if (strategy != null) {
            testStrategy(strategy);
        }
    }

    private HardLinkCopyStrategy attemptGetHardLinkCopyStrategy() {
        try {
            return new HardLinkCopyStrategy();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    private static void testStrategy(FileCopyStrategy strategy) throws IOException {
        final File source = new File("source");
        final File dest = new File("dest");
        assertFalse(source.exists());
        assertFalse(dest.exists());
        source.createNewFile();
        assertTrue(source.exists());
        strategy.copy(source, dest);
        assertTrue(dest.exists());
        IOUtil.delete(source);
        IOUtil.delete(dest);
    }

}
