package com.hazelcast.internal.hotrestart.impl.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * File copy strategy which uses hard links from {@link Files#createLink(Path, Path)}.
 * The availability of hard links and usefulness of this strategy depends on whether
 * the requirements of the {@code createLink} method are satisfied.
 * If the source file changes after the copy operation has finished, the contents of the target file will change as well but if
 * the source file is deleted or renamed, the target file will remain unchanged and still existent.
 * Thus this strategy is most useful for files that never change and are copy-on-write.
 */
public class HardLinkCopyStrategy implements FileCopyStrategy {

    @Override
    public void copy(File source, File target) {
        try {
            final File destination = target.isDirectory() ? new File(target, source.getName()) : target;
            Files.createLink(destination.toPath(), source.toPath());
        } catch (IOException | SecurityException e) {
            throw new UnsupportedOperationException("createLink is not suppported", e);
        }
    }
}
