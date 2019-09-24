package com.hazelcast.spi.hotrestart.impl.io;

import com.hazelcast.internal.nio.IOUtil;

import java.io.File;

/**
 * Default file copy strategy which copies the contents of the source. This means that the source file can change after
 * the copy operation has finished and the target file should not change.
 * The target can be a directory or file. If the target is a file, nests the new file under the target directory, otherwise
 * copies to the given target.
 */
public class DefaultCopyStrategy implements FileCopyStrategy {

    @Override
    public void copy(File source, File target) {
        IOUtil.copyFile(source, target, -1);
    }
}
