package com.hazelcast.spi.hotrestart;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileNotFoundException;

import static com.hazelcast.spi.hotrestart.DirectoryLock.lockForDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DirectoryLockTest {

    private static final ILogger logger = Logger.getLogger(DirectoryLockTest.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule(true);

    private File directory;
    private DirectoryLock directoryLock;

    @Before
    public void setUp() {
        directory = hotRestartFolderRule.getBaseDir();
    }

    @After
    public void tearDown() {
        if (directoryLock != null) {
            try {
                directoryLock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void test_lockForDirectory() {
        directoryLock = lockForDirectory(directory, logger);
        assertNotNull(directoryLock);
        assertTrue(directoryLock.getLock().isValid());
        assertEquals(directory, directoryLock.getDir());
    }

    @Test
    public void test_lockForDirectory_whenAlreadyLocked() {
        directoryLock = lockForDirectory(directory, logger);
        expectedException.expect(HotRestartException.class);
        lockForDirectory(directory, logger);
    }

    @Test
    public void test_lockForDirectory_forNonExistingDir() {
        expectedException.expect(HotRestartException.class);
        expectedException.expectCause(Matchers.<Throwable>instanceOf(FileNotFoundException.class));
        directoryLock = lockForDirectory(new File(UuidUtil.newUnsecureUuidString()), logger);
    }

    @Test
    public void test_release() {
        directoryLock = lockForDirectory(directory, logger);
        directoryLock.release();
        assertFalse(directoryLock.getLock().isValid());
    }

}
