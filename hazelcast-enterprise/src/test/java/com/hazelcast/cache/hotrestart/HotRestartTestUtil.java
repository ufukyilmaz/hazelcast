package com.hazelcast.cache.hotrestart;

import org.junit.rules.TestName;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.deleteQuietly;
import static com.hazelcast.nio.IOUtil.toFileName;

public class HotRestartTestUtil {

    private static final AtomicInteger INSTANCE_INDEX = new AtomicInteger();

    public static File isolatedFolder(Class<?> testClass, TestName testName) {
        return new File(toFileName(testClass.getSimpleName() + '_' + testName.getMethodName())).getAbsoluteFile();
    }

    public static void createFolder(File folder) {
        deleteQuietly(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }
    }

    public static File getBaseDir(File baseFolder) {
        return new File(baseFolder, "hz_" + INSTANCE_INDEX.incrementAndGet());
    }
}
