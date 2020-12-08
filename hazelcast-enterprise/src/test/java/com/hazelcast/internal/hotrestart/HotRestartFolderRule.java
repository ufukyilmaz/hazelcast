package com.hazelcast.internal.hotrestart;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.UUID;

import static com.hazelcast.internal.nio.IOUtil.delete;
import static com.hazelcast.internal.nio.IOUtil.toFileName;
import static org.junit.Assert.assertTrue;

/**
 * Similar to {@link org.junit.rules.TemporaryFolder},
 * but instead of creating temporary folder in tmp dir,
 * creates a folder named {@code Classname_MethodName[params]_UUID} inside the working directory.
 */
public class HotRestartFolderRule extends ExternalResource {

    private final boolean mkdir;
    private File baseDir;

    public HotRestartFolderRule() {
        this(false);
    }

    public HotRestartFolderRule(boolean mkdir) {
        this.mkdir = mkdir;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        String dirName = toFileName(description.getTestClass().getSimpleName()) + '_'
                + toFileName(description.getMethodName() + '_' + UUID.randomUUID().toString());
        baseDir = new File(dirName);
        return super.apply(base, description);
    }

    @Override
    protected void before() {
        delete(baseDir);
        if (mkdir) {
            boolean mkdir = baseDir.mkdir();
            assertTrue("Failed to create: " + baseDir.getAbsolutePath(), mkdir);
        }
    }

    @Override
    protected void after() {
        delete(baseDir);
    }

    public File getBaseDir() {
        return baseDir;
    }

}
