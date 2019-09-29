package com.hazelcast.internal.hotrestart.impl.io;

import java.io.File;

/**
 * Defines a specific strategy for copying files. An implementation can use system specific features to provide different
 * behavior, even one where the file contents are not duplicated but rather a new pointer to the same contents are made
 * (hard link).
 */
public interface FileCopyStrategy {
    /**
     * Copy the file from source to target. The implementation can choose the exact way this happens and the target does not
     * necessarily need to contain a copy of the data. Check the implementation for details.
     */
    void copy(File source, File target);
}
